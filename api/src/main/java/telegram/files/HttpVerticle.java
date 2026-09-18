package telegram.files;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.CookieSameSite;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.ServerWebSocketHandshake;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.healthchecks.HealthCheckHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import io.vertx.ext.web.sstore.SessionStore;
import org.drinkless.tdlib.TdApi;
import org.jooq.lambda.function.Function2;
import telegram.files.repository.FileRecord;
import telegram.files.repository.SettingAutoRecords;
import telegram.files.repository.CloudArchiveHistoryJob;
import telegram.files.repository.CloudArchiveRecord;
import telegram.files.repository.CloudArchiveSyncState;
import telegram.files.repository.SettingKey;
import telegram.files.repository.SettingRecord;
import telegram.files.security.OriginPolicy;
import telegram.files.security.SafePathResolver;
import telegram.files.security.SlidingWindowRateLimiter;
import telegram.files.security.auth.AdminAuthModels.AdminPrincipal;
import telegram.files.security.auth.AdminAuthModels.AuthException;
import telegram.files.security.auth.AdminAuthModels.BootstrapState;
import telegram.files.security.auth.AdminAuthModels.IssuedSession;
import telegram.files.security.auth.AdminAuthService;
import telegram.files.share.UnifiedFileDownloadService;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HttpVerticle extends AbstractVerticle {

    private static final Log log = LogFactory.get();

    // session id -> ws handler id
    private static final Map<String, String> clients = new ConcurrentHashMap<>();

    // session id -> telegram verticle
    private final Map<String, TelegramVerticle> sessionTelegramVerticles = new ConcurrentHashMap<>();

    private final Map<Long, Long> cloudArchiveTestTimestamps = new ConcurrentHashMap<>();

    private final List<String> unboundClients = new ArrayList<>();

    private final FileRouteHandler fileRouteHandler = new FileRouteHandler();

    private UnifiedFileDownloadService unifiedFileDownloadService;

    void configureUnifiedFileDownloadService(UnifiedFileDownloadService service) {
        this.unifiedFileDownloadService = service;
    }

    private static final String SESSION_COOKIE_NAME = "tf";

    private static final String ADMIN_SESSION_COOKIE_NAME = "tf_admin";

    private static final String CSRF_COOKIE_NAME = "tf_csrf";

    private static final String AUTH_PRINCIPAL_KEY = "adminPrincipal";

    private final OriginPolicy originPolicy = new OriginPolicy(Config.HTTP_ALLOWED_ORIGINS);

    private final SlidingWindowRateLimiter loginRateLimiter = new SlidingWindowRateLimiter(
            Config.AUTH_LOGIN_ATTEMPTS_PER_MINUTE, Duration.ofMinutes(1)
    );

    private final SlidingWindowRateLimiter fileReadRateLimiter = new SlidingWindowRateLimiter(
            Config.FILE_READS_PER_MINUTE, Duration.ofMinutes(1)
    );

    private AdminAuthService adminAuthService;

    private BootstrapState bootstrapState;

    @Override
    public void start(Promise<Void> startPromise) {
        adminAuthService = new AdminAuthService(vertx, DataVerticle.pool);
        adminAuthService.initialize()
                .onSuccess(state -> {
                    bootstrapState = state;
                    if (state.required()) {
                        System.out.println(
                                "Telegram Files one-time bootstrap code (expires in 15 minutes): "
                                + state.oneTimeToken()
                        );
                    }
                })
                .compose(_ -> initHttpServer())
                .compose(_ -> initTelegramVerticles())
                .compose(_ -> AutomationsHolder.INSTANCE.init())
                .compose(_ -> initCloudArchiveVerticle())
                .compose(_ -> initAutoDownloadVerticle())
                .compose(_ -> initTransferVerticle())
                .compose(_ -> initPreloadMessageVerticle())
                .compose(_ -> initEventConsumer())
                .onSuccess(startPromise::complete)
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        AutomationsHolder.INSTANCE.saveAutoRecords()
                .onComplete(ignore -> {
                    System.out.println("Http verticle stopped!");
                    stopPromise.complete();
                });
    }

    public Future<Void> initHttpServer() {
        int port = config().getInteger("http.port", 8080);
        String host = Config.HTTP_HOST;
        HttpServerOptions options = new HttpServerOptions()
                .setHost(host)
                .setLogActivity(true)
                .setRegisterWebSocketWriteHandlers(true)
                .setMaxWebSocketMessageSize(1024 * 1024)
                .setIdleTimeout(60)
                .setIdleTimeoutUnit(TimeUnit.SECONDS)
                .setPort(port);

        return vertx.createHttpServer(options)
                .webSocketHandshakeHandler(this::handleWebSocketHandshake)
                .requestHandler(initRouter())
                .listen()
                .onSuccess(_ -> log.info("API server started on {}:{}", host, port))
                .onFailure(err -> log.error("Failed to start API server: %s".formatted(err.getMessage())))
                .mapEmpty();
    }

    public Router initRouter() {
        Router router = Router.router(vertx);

        SessionStore sessionStore = LocalSessionStore.create(vertx, SESSION_COOKIE_NAME);
        SessionHandler sessionHandler = SessionHandler.create(sessionStore)
                .setSessionCookieName(SESSION_COOKIE_NAME)
                .setCookieSameSite(CookieSameSite.STRICT)
                .setCookieSecureFlag(Config.HTTP_SECURE_COOKIES);
        router.route()
                .handler(sessionHandler)
                .handler(BodyHandler.create().setBodyLimit(Config.HTTP_BODY_LIMIT_BYTES));

        if (!originPolicy.allowedOrigins().isEmpty()) {
            CorsHandler corsHandler = CorsHandler.create()
                    .allowedMethod(HttpMethod.GET)
                    .allowedMethod(HttpMethod.POST)
                    .allowedMethod(HttpMethod.PUT)
                    .allowedMethod(HttpMethod.PATCH)
                    .allowedMethod(HttpMethod.DELETE)
                    .allowedMethod(HttpMethod.OPTIONS)
                    .allowCredentials(true)
                    .allowedHeader("Content-Type")
                    .allowedHeader("X-CSRF-Token");
            originPolicy.allowedOrigins().forEach(corsHandler::addOrigin);
            router.route().handler(corsHandler);
        }
        router.route().handler(this::handleOrigin);

        HealthChecks hc = HealthChecks.create(vertx);
        hc.register("http-server", Promise::complete);

        router.get("/health").handler(HealthCheckHandler.createWithHealthChecks(hc));
        router.get("/version").handler(ctx -> ctx.json(new JsonObject().put("version", Start.VERSION)));
        router.get("/auth/bootstrap/status").handler(this::handleBootstrapStatus);
        router.post("/auth/bootstrap").handler(this::handleBootstrap);
        router.post("/auth/login").handler(this::handleLogin);
        router.options().handler(ctx -> ctx.response().setStatusCode(204).end());

        router.route()
                .handler(this::handleAuthentication)
                .handler(this::handleCsrf);

        router.get("/auth/session").handler(this::handleSession);
        router.post("/auth/logout").handler(this::handleLogout);
        router.post("/auth/logout-all").handler(this::handleLogoutAll);
        router.post("/auth/password").handler(this::handlePasswordChange);

        router.post("/share/device/authorize").handler(this::handleShareDeviceAuthorize);
        router.get("/share/device/status").handler(ctx -> requestShareCommand(
                ctx, EventEnum.SHARE_DEVICE_STATUS, new JsonObject()
        ));
        router.post("/share/device/cancel").handler(ctx -> requestShareCommand(
                ctx, EventEnum.SHARE_DEVICE_CANCEL, new JsonObject()
        ));
        router.delete("/share/node").handler(ctx -> requestShareCommand(
                ctx, EventEnum.SHARE_NODE_UNBIND, new JsonObject()
        ));
        router.put("/share/node/name").handler(this::handleShareNodeRename);
        router.get("/share/resources").handler(this::handleShareResourceList);
        router.get("/share/publication-policy").handler(ctx -> requestShareCommand(
                ctx, EventEnum.SHARE_PUBLICATION_POLICY, new JsonObject()
        ));
        router.post("/share/resources").handler(this::handleShareResourcePublish);
        router.put("/share/resources/:sourceId").handler(this::handleShareResourceUpdate);
        router.delete("/share/resources/:sourceId").handler(this::handleShareResourceRevoke);

        router.get("/").handler(ctx -> ctx.response().end("Hello World!"));
        router.get("/settings").handler(this::handleSettings);
        router.post("/settings/create").handler(this::handleSettingsCreate);
        router.get("/automations/chats").handler(this::handleAutomationChats);
        router.get("/cloud-archive/overview").handler(this::handleCloudArchiveOverview);
        router.get("/cloud-archive/records").handler(this::handleCloudArchiveRecords);
        router.get("/cloud-archive/history").handler(this::handleCloudArchiveHistory);
        router.post("/cloud-archive/history").handler(this::handleCloudArchiveHistoryCreate);
        router.post("/cloud-archive/history/:jobId/daily-limit").handler(this::handleCloudArchiveHistoryDailyLimit);
        router.post("/cloud-archive/history/:jobId/:action").handler(this::handleCloudArchiveHistoryAction);
        router.post("/cloud-archive/validate").handler(this::handleCloudArchiveValidate);
        router.post("/cloud-archive/test").handler(this::handleCloudArchiveTest);
        router.post("/cloud-archive/records/retry-all").handler(this::handleCloudArchiveRetryAll);
        router.post("/cloud-archive/records/clear").handler(this::handleCloudArchiveClearRecords);
        router.post("/cloud-archive/records/:recordId/retry").handler(this::handleCloudArchiveRetry);
        router.get("/local-organize/overview").handler(this::handleLocalOrganizeOverview);
        router.get("/local-organize/sources").handler(this::handleLocalOrganizeSources);
        router.post("/local-organize/preview").handler(this::handleLocalOrganizePreview);

        router.post("/telegram/create").handler(this::handleTelegramCreate);
        router.post("/telegram/:telegramId/delete").handler(this::handleTelegramDelete);
        router.get("/telegram/api/methods").handler(this::handleTelegramApiMethods);
        router.get("/telegram/api/:method/parameters").handler(this::handleTelegramApiMethodParameters);
        router.post("/telegram/api/:method").handler(this::handleTelegramApi);
        router.get("/telegrams").handler(this::handleTelegrams);
        router.get("/telegram/:telegramId/chats").handler(this::handleTelegramChats);
        router.get("/telegram/:telegramId/chat/:chatId/topics").handler(this::handleTelegramTopics);
        router.get("/telegram/:telegramId/chat/:chatId/files").handler(this::handleTelegramFiles);
        router.get("/telegram/:telegramId/chat/:chatId/files/count").handler(this::handleTelegramFilesCount);
        router.get("/telegram/:telegramId/download-statistics").handler(this::handleTelegramDownloadStatistics);
        router.post("/telegrams/change").handler(this::handleTelegramChange);
        router.post("/telegram/:telegramId/toggle-proxy").handler(this::handleTelegramToggleProxy);
        router.get("/telegram/:telegramId/ping").handler(this::handleTelegramPing);
        router.get("/telegram/:telegramId/test-network").handler(this::handleTelegramTestNetwork);

        router.get("/:telegramId/file/:uniqueId").handler(this::handleFilePreview);
        router.post("/:telegramId/file/start-download").handler(this::handleFileStartDownload);
        router.post("/:telegramId/file/cancel-download").handler(this::handleFileCancelDownload);
        router.post("/:telegramId/file/toggle-pause-download").handler(this::handleFileTogglePauseDownload);
        router.post("/:telegramId/file/remove").handler(this::handleFileRemove);
        router.post("/:telegramId/file/update-auto-settings").handler(this::handleAutoSettingsUpdate);

        router.get("/files/count").handler(this::handleFilesCount);
        router.get("/files/chats").handler(this::handleFilesChats);
        router.get("/files").handler(this::handleFiles);
        router.post("/files/start-download-multiple").handler(this::handleFileStartDownloadMultiple);
        router.post("/files/cancel-download-multiple").handler(this::handleFileCancelDownloadMultiple);
        router.post("/files/toggle-pause-download-multiple").handler(this::handleFileTogglePauseDownloadMultiple);
        router.post("/files/set-upload-limit-multiple").handler(this::handleFileSetUploadLimitMultiple);
        router.post("/files/remove-multiple").handler(this::handleFileRemoveMultiple);
        router.post("/files/update-tags").handler(this::handleFileTagsUpdateMultiple);
        router.post("/files/transfer-multiple").handler(this::handleFileTransferMultiple);
        router.post("/file/:uniqueId/update-tags").handler(this::handleFileTagsUpdate);

        router.route()
                .failureHandler(ctx -> {
                    int statusCode = ctx.statusCode();
                    if (statusCode < 500) {
                        if (ctx.response().ended()) {
                            return;
                        }
                        ctx.response().setStatusCode(statusCode).end();
                        return;
                    }
                    Throwable throwable = ctx.failure();
                    log.trace("route: %s, statusCode: %d".formatted(
                            ctx.request().path(),
                            statusCode), throwable);
                    HttpServerResponse response = ctx.response();
                    response.setStatusCode(statusCode)
                            .putHeader("Content-Type", "application/json")
                            .end(JsonObject.of("error", throwable == null ? "☹️Sorry! Not today." : throwable.getMessage()).encode());
                });
        return router;
    }

    public Future<Void> initTelegramVerticles() {
        return TelegramVerticles.initTelegramVerticles(vertx);
    }

    public Future<Void> initAutoDownloadVerticle() {
        return vertx.deployVerticle(new AutoDownloadVerticle(), Config.VIRTUAL_THREAD_DEPLOYMENT_OPTIONS)
                .mapEmpty();
    }

    public Future<Void> initCloudArchiveVerticle() {
        return vertx.deployVerticle(new AutoCloudArchiveVerticle(), Config.VIRTUAL_THREAD_DEPLOYMENT_OPTIONS)
                .mapEmpty();
    }

    public Future<Void> initTransferVerticle() {
        return vertx.deployVerticle(new TransferVerticle(), Config.VIRTUAL_THREAD_DEPLOYMENT_OPTIONS)
                .mapEmpty();
    }

    public Future<Void> initPreloadMessageVerticle() {
        return vertx.deployVerticle(new PreloadMessageVerticle(), Config.VIRTUAL_THREAD_DEPLOYMENT_OPTIONS)
                .mapEmpty();
    }

    private Future<Void> initEventConsumer() {
        vertx.eventBus().consumer(EventEnum.TELEGRAM_EVENT.address(), message -> {
            log.debug("Received telegram event: %s".formatted(message.body()));
            JsonObject jsonObject = (JsonObject) message.body();
            String telegramId = jsonObject.getString("telegramId");
            EventPayload payload = jsonObject.getJsonObject("payload").mapTo(EventPayload.class);

            Set<String> sentSessionIds = new HashSet<>();
            sessionTelegramVerticles.entrySet().stream()
                    .filter(e -> Objects.equals(Convert.toStr(e.getValue().getId()), telegramId))
                    .map(Map.Entry::getKey)
                    .forEach(sessionId -> {
                        String wsHandlerId = clients.get(sessionId);
                        if (StrUtil.isNotBlank(wsHandlerId)) {
                            vertx.eventBus().send(wsHandlerId, Json.encode(payload));
                        }
                        sentSessionIds.add(sessionId);
                    });

            unboundClients.forEach(sessionId -> {
                if (sentSessionIds.contains(sessionId)) {
                    return;
                }
                String wsHandlerId = clients.get(sessionId);
                if (StrUtil.isNotBlank(wsHandlerId)) {
                    vertx.eventBus().send(wsHandlerId, Json.encode(payload));
                }
            });
        });

        vertx.eventBus().consumer(EventEnum.AUTO_DOWNLOAD_UPDATE.address(), message -> {
            log.debug("Auto settings update: %s".formatted(message.body()));
            AutomationsHolder.INSTANCE.onAutoRecordsUpdate(Json.decodeValue(message.body().toString(), SettingAutoRecords.class));
        });
        return Future.succeededFuture();
    }

    private void handleOrigin(RoutingContext ctx) {
        String origin = ctx.request().getHeader("Origin");
        boolean configuredOrigin = originPolicy.isAllowed(origin);
        boolean implicitSameOrigin = originPolicy.allowedOrigins().isEmpty()
                                     && isSameOriginRequest(ctx, origin);
        if (!configuredOrigin && !implicitSameOrigin) {
            respondJson(ctx, 403, "ORIGIN_NOT_ALLOWED", "Request origin is not allowed");
            return;
        }
        ctx.next();
    }

    private static boolean isSameOriginRequest(RoutingContext ctx, String rawOrigin) {
        if (StrUtil.isBlank(rawOrigin)) {
            return true;
        }
        try {
            URI origin = URI.create(rawOrigin);
            String directHost = ctx.request().remoteAddress() == null
                    ? "unknown"
                    : ctx.request().remoteAddress().host();
            boolean trustedProxy = OriginPolicy.isLoopback(directHost);
            String expectedScheme = trustedProxy
                    ? ctx.request().getHeader("X-Forwarded-Proto")
                    : null;
            expectedScheme = StrUtil.blankToDefault(expectedScheme, "http");
            String expectedAuthority = trustedProxy
                    ? ctx.request().getHeader("X-Forwarded-Host")
                    : null;
            expectedAuthority = StrUtil.blankToDefault(
                    expectedAuthority,
                    ctx.request().getHeader("Host")
            );
            return expectedAuthority != null
                   && expectedScheme.equalsIgnoreCase(origin.getScheme())
                   && expectedAuthority.equalsIgnoreCase(origin.getRawAuthority());
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private void handleBootstrapStatus(RoutingContext ctx) {
        adminAuthService.bootstrapRequired()
                .onSuccess(required -> ctx.json(JsonObject.of("required", required)))
                .onFailure(failure -> respondFailure(ctx, failure));
    }

    private void handleBootstrap(RoutingContext ctx) {
        String source = remoteHost(ctx);
        if (!acquire(loginRateLimiter, "bootstrap:" + source, ctx)) {
            return;
        }
        JsonObject body = requestBody(ctx);
        if (body == null) {
            return;
        }
        String bootstrapToken = body.getString("bootstrapToken");
        String username = body.getString("username");
        String password = body.getString("password");
        adminAuthService.bootstrap(
                        bootstrapToken,
                        username,
                        password == null ? null : password.toCharArray(),
                        OriginPolicy.isLocalNetwork(source)
                )
                .onSuccess(session -> {
                    addSessionCookies(ctx, session);
                    ctx.response().setStatusCode(201).end(sessionBody(session).encode());
                })
                .onFailure(failure -> respondFailure(ctx, failure));
    }

    private void handleLogin(RoutingContext ctx) {
        JsonObject body = requestBody(ctx);
        if (body == null) {
            return;
        }
        String username = body.getString("username");
        String source = remoteHost(ctx);
        if (!acquire(
                loginRateLimiter,
                "login:" + source + ":" + String.valueOf(username).toLowerCase(Locale.ROOT),
                ctx
        )) {
            return;
        }
        String password = body.getString("password");
        adminAuthService.login(
                        username,
                        password == null ? null : password.toCharArray(),
                        source
                )
                .onSuccess(session -> {
                    addSessionCookies(ctx, session);
                    ctx.response().end(sessionBody(session).encode());
                })
                .onFailure(failure -> respondFailure(ctx, failure));
    }

    private void handleAuthentication(RoutingContext ctx) {
        Cookie cookie = ctx.request().getCookie(ADMIN_SESSION_COOKIE_NAME);
        String token = cookie != null ? cookie.getValue() : ctx.request().getParam("token");
        if (StrUtil.isBlank(token)) {
            String authHeader = ctx.request().getHeader("Authorization");
            if (StrUtil.isNotBlank(authHeader) && authHeader.startsWith("Bearer ")) {
                token = authHeader.substring(7).trim();
            }
        }
        if (StrUtil.isBlank(token)) {
            respondJson(ctx, 401, "AUTHENTICATION_REQUIRED", "Authentication is required");
            return;
        }
        adminAuthService.authenticate(token)
                .onSuccess(principal -> {
                    ctx.put(AUTH_PRINCIPAL_KEY, principal);
                    ctx.next();
                })
                .onFailure(failure -> {
                    clearSessionCookies(ctx);
                    respondFailure(ctx, failure);
                });
    }

    private void handleCsrf(RoutingContext ctx) {
        if (Set.of(HttpMethod.GET, HttpMethod.HEAD, HttpMethod.OPTIONS)
                .contains(ctx.request().method())) {
            ctx.next();
            return;
        }
        AdminPrincipal principal = principal(ctx);
        if (!adminAuthService.validateCsrf(
                principal,
                ctx.request().getHeader("X-CSRF-Token")
        )) {
            log.debug("Rejected request: CSRF token is invalid for administrator %s".formatted(principal.username()));
            respondJson(ctx, 403, "CSRF_TOKEN_INVALID", "CSRF token is invalid");
            return;
        }
        ctx.next();
    }

    private void handleSession(RoutingContext ctx) {
        AdminPrincipal principal = principal(ctx);
        Cookie cookie = ctx.request().getCookie(ADMIN_SESSION_COOKIE_NAME);
        String token = cookie != null ? cookie.getValue() : ctx.request().getParam("token");
        ctx.json(JsonObject.of(
                "authenticated", true,
                "token", token == null ? "" : token,
                "username", principal.username(),
                "idleExpiresAt", principal.idleExpiresAt(),
                "absoluteExpiresAt", principal.absoluteExpiresAt()
        ));
    }

    private void handleLogout(RoutingContext ctx) {
        AdminPrincipal principal = principal(ctx);
        adminAuthService.logout(principal)
                .onSuccess(_ -> {
                    clearSessionCookies(ctx);
                    ctx.response().setStatusCode(204).end();
                })
                .onFailure(failure -> respondFailure(ctx, failure));
    }

    private void handleLogoutAll(RoutingContext ctx) {
        AdminPrincipal principal = principal(ctx);
        adminAuthService.logoutAll(principal)
                .onSuccess(_ -> {
                    clearSessionCookies(ctx);
                    ctx.response().setStatusCode(204).end();
                })
                .onFailure(failure -> respondFailure(ctx, failure));
    }

    private void handlePasswordChange(RoutingContext ctx) {
        JsonObject body = requestBody(ctx);
        if (body == null) {
            return;
        }
        String currentPassword = body.getString("currentPassword");
        String newPassword = body.getString("newPassword");
        adminAuthService.changePassword(
                        principal(ctx),
                        currentPassword == null ? null : currentPassword.toCharArray(),
                        newPassword == null ? null : newPassword.toCharArray()
                )
                .onSuccess(_ -> {
                    clearSessionCookies(ctx);
                    ctx.response().setStatusCode(204).end();
                })
                .onFailure(failure -> respondFailure(ctx, failure));
    }

    private void handleShareDeviceAuthorize(RoutingContext ctx) {
        JsonObject body = requestBody(ctx);
        if (body != null) {
            requestShareCommand(ctx, EventEnum.SHARE_DEVICE_AUTHORIZE, body);
        }
    }

    private void handleShareNodeRename(RoutingContext ctx) {
        JsonObject body = requestBody(ctx);
        if (body != null) {
            requestShareCommand(ctx, EventEnum.SHARE_NODE_RENAME, body);
        }
    }

    private void handleShareResourcePublish(RoutingContext ctx) {
        JsonObject body = requestBody(ctx);
        if (body != null) {
            requestShareCommand(ctx, EventEnum.SHARE_RESOURCE_PUBLISH, body);
        }
    }

    private void handleShareResourceList(RoutingContext ctx) {
        requestShareCommand(
                ctx,
                EventEnum.SHARE_RESOURCE_LIST,
                new JsonObject()
                        .put("page", Convert.toInt(ctx.request().getParam("page"), 1))
                        .put("pageSize", Convert.toInt(ctx.request().getParam("pageSize"), 10))
        );
    }

    private void handleShareResourceUpdate(RoutingContext ctx) {
        JsonObject body = requestBody(ctx);
        if (body != null) {
            body.put("sourceId", ctx.pathParam("sourceId"));
            requestShareCommand(ctx, EventEnum.SHARE_RESOURCE_UPDATE, body);
        }
    }

    private void handleShareResourceRevoke(RoutingContext ctx) {
        requestShareCommand(
                ctx,
                EventEnum.SHARE_RESOURCE_REVOKE,
                new JsonObject().put("sourceId", ctx.pathParam("sourceId"))
        );
    }

    private void requestShareCommand(RoutingContext ctx, EventEnum event, JsonObject body) {
        if (!Config.shareConfiguration().enabled()) {
            respondJson(ctx, 404, "SHARE_DISABLED", "Share module is disabled");
            return;
        }
        vertx.eventBus().<JsonObject>request(event.address(), body)
                .onSuccess(message -> ctx.json(message.body()))
                .onFailure(failure -> respondJson(
                        ctx,
                        400,
                        "SHARE_COMMAND_FAILED",
                        failure.getMessage() == null ? "Share command failed" : failure.getMessage()
                ));
    }

    private boolean acquire(
            SlidingWindowRateLimiter limiter,
            String key,
            RoutingContext ctx
    ) {
        if (limiter.tryAcquire(key)) {
            return true;
        }
        ctx.response().putHeader(
                "Retry-After",
                String.valueOf(limiter.retryAfterSeconds(key))
        );
        respondJson(ctx, 429, "RATE_LIMITED", "Too many requests");
        return false;
    }

    private JsonObject requestBody(RoutingContext ctx) {
        try {
            JsonObject body = ctx.body().asJsonObject();
            if (body == null) {
                respondJson(ctx, 400, "REQUEST_BODY_REQUIRED", "JSON request body is required");
            }
            return body;
        } catch (RuntimeException exception) {
            respondJson(ctx, 400, "REQUEST_BODY_INVALID", "JSON request body is invalid");
            return null;
        }
    }

    private void addSessionCookies(RoutingContext ctx, IssuedSession session) {
        long maxAgeSeconds = Math.max(
                1,
                (session.principal().absoluteExpiresAt() - System.currentTimeMillis()) / 1_000
        );
        ctx.response().addCookie(
                Cookie.cookie(ADMIN_SESSION_COOKIE_NAME, session.sessionToken())
                        .setHttpOnly(true)
                        .setSecure(Config.HTTP_SECURE_COOKIES)
                        .setSameSite(CookieSameSite.STRICT)
                        .setPath("/")
                        .setMaxAge(maxAgeSeconds)
        );
        ctx.response().addCookie(
                Cookie.cookie(CSRF_COOKIE_NAME, session.csrfToken())
                        .setHttpOnly(false)
                        .setSecure(Config.HTTP_SECURE_COOKIES)
                        .setSameSite(CookieSameSite.STRICT)
                        .setPath("/")
                        .setMaxAge(maxAgeSeconds)
        );
    }

    private static void clearSessionCookies(RoutingContext ctx) {
        ctx.response().addCookie(
                Cookie.cookie(ADMIN_SESSION_COOKIE_NAME, "")
                        .setHttpOnly(true)
                        .setSecure(Config.HTTP_SECURE_COOKIES)
                        .setSameSite(CookieSameSite.STRICT)
                        .setPath("/")
                        .setMaxAge(0)
        );
        ctx.response().addCookie(
                Cookie.cookie(CSRF_COOKIE_NAME, "")
                        .setHttpOnly(false)
                        .setSecure(Config.HTTP_SECURE_COOKIES)
                        .setSameSite(CookieSameSite.STRICT)
                        .setPath("/")
                        .setMaxAge(0)
        );
    }

    private static JsonObject sessionBody(IssuedSession session) {
        return JsonObject.of(
                "authenticated", true,
                "token", session.sessionToken(),
                "username", session.principal().username(),
                "idleExpiresAt", session.principal().idleExpiresAt(),
                "absoluteExpiresAt", session.principal().absoluteExpiresAt()
        );
    }

    private static AdminPrincipal principal(RoutingContext ctx) {
        return ctx.get(AUTH_PRINCIPAL_KEY);
    }

    private static String remoteHost(RoutingContext ctx) {
        String directHost = ctx.request().remoteAddress() == null
                ? "unknown"
                : ctx.request().remoteAddress().host();
        if (OriginPolicy.isLoopback(directHost)) {
            String proxiedHost = ctx.request().getHeader("X-Real-IP");
            if (StrUtil.isNotBlank(proxiedHost)) {
                return proxiedHost.trim();
            }
        }
        return directHost;
    }

    private static void respondJson(
            RoutingContext ctx,
            int statusCode,
            String errorCode,
            String message
    ) {
        if (ctx.response().ended()) {
            return;
        }
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject.of(
                        "error", JsonObject.of(
                                "code", errorCode,
                                "message", message
                        )
                ).encode());
    }

    private static void respondFailure(RoutingContext ctx, Throwable failure) {
        if (failure instanceof AuthException authException) {
            respondJson(
                    ctx,
                    authException.statusCode(),
                    authException.errorCode(),
                    authException.getMessage()
            );
            return;
        }
        log.error("Security request failed", failure);
        respondJson(ctx, 500, "INTERNAL_ERROR", "The request could not be completed");
    }

    private void handleWebSocketHandshake(ServerWebSocketHandshake handshake) {
        if (!"/ws".equals(handshake.path())) {
            rejectWebSocket(handshake, 404, "WebSocket path is not supported");
            return;
        }
        if (!isWebSocketOriginAllowed(handshake)) {
            rejectWebSocket(handshake, 403, "WebSocket origin is not allowed");
            return;
        }

        String adminSessionToken = cookieValue(handshake.headers(), ADMIN_SESSION_COOKIE_NAME);
        if (StrUtil.isBlank(adminSessionToken)) {
            rejectWebSocket(handshake, 401, "administrator session cookie is missing");
            return;
        }

        String webSessionId = cookieValue(handshake.headers(), SESSION_COOKIE_NAME);
        String telegramId = queryParameter(handshake.query(), "telegramId");
        adminAuthService.authenticate(adminSessionToken)
                .onSuccess(adminPrincipal -> {
                    String sessionId = StrUtil.blankToDefault(webSessionId, adminPrincipal.sessionId());
                    handshake.accept()
                            .onSuccess(ws -> initializeWebSocket(ws, adminPrincipal, sessionId, telegramId))
                            .onFailure(failure -> log.error("Failed to accept authenticated WebSocket", failure));
                })
                .onFailure(failure -> {
                    int statusCode = failure instanceof AuthException authException
                            ? authException.statusCode()
                            : 500;
                    rejectWebSocket(handshake, statusCode, "administrator session cookie is invalid or expired");
                });
    }

    private boolean isWebSocketOriginAllowed(ServerWebSocketHandshake handshake) {
        String origin = handshake.headers().get("Origin");
        if (originPolicy.isAllowed(origin)) {
            return true;
        }
        return originPolicy.allowedOrigins().isEmpty()
               && isSameOriginHandshake(handshake, origin);
    }

    private static boolean isSameOriginHandshake(ServerWebSocketHandshake handshake, String rawOrigin) {
        if (StrUtil.isBlank(rawOrigin)) {
            return true;
        }
        try {
            URI origin = URI.create(rawOrigin);
            String directHost = handshake.remoteAddress() == null
                    ? "unknown"
                    : handshake.remoteAddress().host();
            boolean trustedProxy = OriginPolicy.isLoopback(directHost);
            String expectedScheme = trustedProxy
                    ? handshake.headers().get("X-Forwarded-Proto")
                    : null;
            expectedScheme = StrUtil.blankToDefault(expectedScheme, handshake.scheme());
            expectedScheme = StrUtil.blankToDefault(
                    expectedScheme,
                    handshake.isSsl() ? "https" : "http"
            );
            String expectedAuthority = trustedProxy
                    ? handshake.headers().get("X-Forwarded-Host")
                    : null;
            expectedAuthority = StrUtil.blankToDefault(
                    expectedAuthority,
                    handshake.headers().get("Host")
            );
            return expectedAuthority != null
                   && expectedScheme.equalsIgnoreCase(origin.getScheme())
                   && expectedAuthority.equalsIgnoreCase(origin.getRawAuthority());
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private static void rejectWebSocket(
            ServerWebSocketHandshake handshake,
            int statusCode,
            String reason
    ) {
        handshake.reject(statusCode)
                .onFailure(failure -> log.error("Failed to reject WebSocket handshake", failure));
    }

    static String cookieValue(MultiMap headers, String name) {
        for (String header : headers.getAll("Cookie")) {
            for (String cookie : header.split(";")) {
                int separator = cookie.indexOf('=');
                if (separator <= 0 || !name.equals(cookie.substring(0, separator).trim())) {
                    continue;
                }
                String value = cookie.substring(separator + 1).trim();
                if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                    return value.substring(1, value.length() - 1);
                }
                return value;
            }
        }
        return null;
    }

    static String queryParameter(String query, String name) {
        if (StrUtil.isBlank(query)) {
            return null;
        }
        for (String parameter : query.split("&")) {
            int separator = parameter.indexOf('=');
            String key = separator < 0 ? parameter : parameter.substring(0, separator);
            if (!name.equals(URLUtil.decode(key))) {
                continue;
            }
            return separator < 0 ? "" : URLUtil.decode(parameter.substring(separator + 1));
        }
        return null;
    }

    private void initializeWebSocket(
            ServerWebSocket ws,
            AdminPrincipal adminPrincipal,
            String sessionId,
            String telegramId
    ) {
        try {
            String textHandlerId = ws.textHandlerID();
            if (textHandlerId == null) {
                log.error("Failed to initialize authenticated WebSocket: text handler is unavailable");
                ws.close();
                return;
            }
            clients.put(sessionId, textHandlerId);
            if (!handleTelegramChange(sessionId, telegramId)) {
                log.debug("Failed to change Telegram account for WebSocket connection");
            }
            if (StrUtil.isBlank(telegramId)) {
                if (!unboundClients.contains(sessionId)) {
                    unboundClients.add(sessionId);
                }
            } else {
                unboundClients.remove(sessionId);
            }

            long timerId = vertx.setPeriodic(30000, _ -> {
                if (ws.isClosed()) {
                    return;
                }
                adminAuthService.isSessionActive(adminPrincipal.sessionId())
                        .onSuccess(active -> {
                            if (!active) {
                                ws.close();
                                return;
                            }
                            ws.writePing(Buffer.buffer("👀"));
                        })
                        .onFailure(_ -> ws.close());
            });

            ws.exceptionHandler(throwable -> log.error("WebSocket error: %s".formatted(throwable.getMessage())));
            ws.closeHandler(_ -> {
                TelegramVerticle selected = sessionTelegramVerticles.get(sessionId);
                if (selected != null) selected.releaseFrontendSession(sessionId);
                clients.remove(sessionId, textHandlerId);
                sessionTelegramVerticles.remove(sessionId);
                unboundClients.remove(sessionId);
                vertx.cancelTimer(timerId);
            });

            ws.textMessageHandler(text -> log.debug("Received WebSocket message: " + text));
        } catch (RuntimeException exception) {
            log.error("Failed to initialize authenticated WebSocket", exception);
            ws.close();
        }
    }

    private void handleSettingsCreate(RoutingContext ctx) {
        JsonObject object = ctx.body().asJsonObject();
        if (CollUtil.isEmpty(object)) {
            ctx.fail(400);
            return;
        }

        Future.all(object.stream()
                        .map(setting -> DataVerticle.settingRepository.createOrUpdate(setting.getKey(),
                                Convert.toStr(setting.getValue(), "")))
                        .toList())
                .map(CompositeFuture::<SettingRecord>list)
                .onSuccess(records -> {
                    records.forEach(record ->
                            vertx.eventBus().publish(EventEnum.SETTING_UPDATE.address(record.key()), record.value()));
                    ctx.end();
                })
                .onFailure(ctx::fail);
    }

    private void handleSettings(RoutingContext ctx) {
        String keysStr = ctx.request().getParam("keys");
        if (StrUtil.isBlank(keysStr)) {
            ctx.fail(400);
            return;
        }
        List<String> keys = Arrays.asList(keysStr.split(","));
        DataVerticle.settingRepository
                .getByKeys(keys)
                .onSuccess(settings -> {
                    JsonObject object = new JsonObject();
                    for (SettingRecord record : settings) {
                        object.put(record.key(), record.value());
                    }
                    for (String key : keys) {
                        if (object.containsKey(key)) {
                            continue;
                        }
                        if ("shareEnabled".equals(key)) {
                            object.put("shareEnabled", Config.shareConfiguration().enabled());
                            continue;
                        }
                        object.put(key, SettingKey.valueOf(key).defaultValue);
                    }
                    if (keys.contains("shareEnabled")) {
                        object.put("shareEnabled", Config.shareConfiguration().enabled());
                    }
                    ctx.json(object);
                })
                .onFailure(ctx::fail);
    }

    private void handleTelegramCreate(RoutingContext ctx) {
        String sessionId = ctx.session().id();
        TelegramVerticle telegramVerticle = sessionTelegramVerticles.get(sessionId);
        boolean isClosedOrClosing = telegramVerticle != null &&
                                    telegramVerticle.lastAuthorizationState != null &&
                                    (telegramVerticle.lastAuthorizationState.getConstructor() == TdApi.AuthorizationStateClosed.CONSTRUCTOR ||
                                     telegramVerticle.lastAuthorizationState.getConstructor() == TdApi.AuthorizationStateClosing.CONSTRUCTOR);
        if (telegramVerticle != null && !telegramVerticle.authorized && !isClosedOrClosing) {
            ctx.json(new JsonObject()
                    .put("id", telegramVerticle.getId())
                    .put("lastState", telegramVerticle.lastAuthorizationState)
            );
            return;
        }
        JsonObject jsonObject = ctx.body().asJsonObject();
        String proxyName = jsonObject.getString("proxyName");

        TelegramVerticle newTelegramVerticle = TelegramVerticles.create(DataVerticle.telegramRepository.getRootPath());
        newTelegramVerticle.setProxy(proxyName);
        sessionTelegramVerticles.put(sessionId, newTelegramVerticle);
        TelegramVerticles.add(newTelegramVerticle);
        vertx.deployVerticle(newTelegramVerticle)
                .onSuccess(_ -> ctx.json(new JsonObject()
                        .put("id", newTelegramVerticle.getId())
                        .put("lastState", newTelegramVerticle.lastAuthorizationState)
                ))
                .onFailure(ctx::fail);
    }

    private void handleTelegramDelete(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = getTelegramVerticleByPath(ctx);
        if (telegramVerticle == null) {
            return;
        }
        telegramVerticle.close(true)
                .onSuccess(_ -> {
                    TelegramVerticles.remove(telegramVerticle);
                    sessionTelegramVerticles.entrySet().removeIf(e -> {
                        if (!e.getValue().equals(telegramVerticle)) return false;
                        telegramVerticle.releaseFrontendSession(e.getKey());
                        return true;
                    });
                    ctx.end();
                })
                .onFailure(ctx::fail);
    }

    private void handleTelegrams(RoutingContext ctx) {
        Boolean authorized = Convert.toBool(ctx.request().getParam("authorized"));
        Future.all(TelegramVerticles.getAll().stream()
                        .filter(c -> authorized == null || c.authorized == authorized)
                        .map(TelegramVerticle::getTelegramAccount)
                        .toList()
                )
                .map(CompositeFuture::list)
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleTelegramChats(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = getTelegramVerticleByPath(ctx);
        if (telegramVerticle == null) {
            return;
        }
        String query = ctx.request().getParam("query");
        String chatId = ctx.request().getParam("chatId");
        String archived = ctx.request().getParam("archived");
        telegramVerticle.getChats(Convert.toLong(chatId), query, Convert.toBool(archived, false))
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleAutomationChats(RoutingContext ctx) {
        List<SettingAutoRecords.Automation> enabledAutomations = AutomationsHolder.INSTANCE.autoRecords().automations.stream()
                .filter(automation -> (automation.preload != null && automation.preload.enabled)
                                      || (automation.download != null && automation.download.enabled)
                                      || (automation.transfer != null && automation.transfer.enabled)
                                      || (automation.archive != null && automation.archive.rule != null
                                          && automation.archive.rule.targetChatId != 0)
                                      || (automation.transfer != null && automation.transfer.rule != null
                                          && StrUtil.isNotBlank(automation.transfer.rule.destination)))
                .toList();

        List<JsonObject> overviewItems = enabledAutomations.stream()
                .map(automation -> {
                    Optional<TelegramVerticle> telegramVerticleOptional = TelegramVerticles.get(automation.telegramId);
                    JsonObject item = new JsonObject()
                            .put("telegramId", Convert.toStr(automation.telegramId))
                            .put("chatId", Convert.toStr(automation.chatId))
                            .put("auto", automation);

                    telegramVerticleOptional.ifPresent(telegramVerticle -> {
                        item.put("accountName", accountDisplayName(telegramVerticle));

                        TdApi.Chat chat = telegramVerticle.getChat(automation.chatId);
                        if (chat != null) {
                            item.put("chatName", chat.id == automation.telegramId ? "Saved Messages" : chat.title)
                                    .put("chatType", TdApiHelp.getChatType(chat.type))
                                    .put("chatAvatar", minithumbnail(chat))
                                    .put("unreadCount", chat.unreadCount);
                        }
                    });

                    item.put("accountName", item.getString("accountName", item.getString("telegramId")))
                            .put("chatName", item.getString("chatName", item.getString("chatId")))
                            .put("chatType", item.getString("chatType", "unknown"))
                            .put("chatAvatar", item.getString("chatAvatar", ""));
                    return item;
                })
                .sorted(Comparator.comparing((JsonObject item) -> item.getString("accountName", ""))
                        .thenComparing(item -> item.getString("chatName", "")))
                .toList();

        ctx.json(new JsonArray(overviewItems));
    }

    private void handleCloudArchiveOverview(RoutingContext ctx) {
        List<JsonObject> rules = AutomationsHolder.INSTANCE.autoRecords().getArchiveConfiguredItems().stream()
                .map(automation -> {
                    JsonObject item = new JsonObject()
                            .put("telegramId", Convert.toStr(automation.telegramId))
                            .put("sourceChatId", Convert.toStr(automation.chatId))
                            .put("targetChatId", Convert.toStr(automation.archive.rule.targetChatId))
                            .put("enabled", automation.archive.enabled)
                            .put("rule", automation.archive.rule);
                    TelegramVerticles.get(automation.telegramId).ifPresent(telegram -> {
                        item.put("accountName", accountDisplayName(telegram));
                        TdApi.Chat source = telegram.getChat(automation.chatId);
                        TdApi.Chat target = telegram.getChat(automation.archive.rule.targetChatId);
                        item.put("sourceChatName", source == null ? Convert.toStr(automation.chatId) : source.title)
                                .put("targetChatName", target == null
                                        ? Convert.toStr(automation.archive.rule.targetChatId)
                                        : target.title)
                                .put("sourceIsForum", telegram.isForum(automation.chatId))
                                .put("targetIsForum", telegram.isForum(automation.archive.rule.targetChatId));
                    });
                    SettingAutoRecords.Automation targetAutomation = AutomationsHolder.INSTANCE.autoRecords()
                            .getItem(automation.telegramId, automation.archive.rule.targetChatId);
                    item.put("targetDownloadEnabled", targetAutomation != null
                            && targetAutomation.download != null && targetAutomation.download.enabled);
                    return item
                            .put("accountName", item.getString("accountName", item.getString("telegramId")))
                            .put("sourceChatName", item.getString("sourceChatName", item.getString("sourceChatId")))
                            .put("targetChatName", item.getString("targetChatName", item.getString("targetChatId")));
                })
                .sorted(Comparator.comparing(item -> item.getString("sourceChatName", "")))
                .toList();
        Future.all(rules.stream().map(this::withCloudArchiveSync).toList())
                .compose(enriched -> DataVerticle.cloudArchiveRepository.statistics()
                        .map(statistics -> {
                            long now = System.currentTimeMillis();
                            JsonObject cooldowns = new JsonObject();
                            AutoCloudArchiveVerticle.getActiveCooldowns().forEach((telegramId, until) -> {
                                if (until > now) {
                                    long remainingSec = Math.max(1, (until - now) / 1000L);
                                    cooldowns.put(String.valueOf(telegramId), new JsonObject()
                                            .put("cooldownUntil", until)
                                            .put("remainingSeconds", remainingSec));
                                }
                            });
                            return new JsonObject()
                                    .put("statistics", statistics)
                                    .put("rules", new JsonArray(rules))
                                    .put("accountCooldowns", cooldowns);
                        }))
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private Future<JsonObject> withCloudArchiveSync(JsonObject item) {
        long telegramId = Convert.toLong(item.getValue("telegramId"));
        long sourceChatId = Convert.toLong(item.getValue("sourceChatId"));
        long targetChatId = Convert.toLong(item.getValue("targetChatId"));
        return DataVerticle.cloudArchiveSyncRepository
                .listRoute(telegramId, sourceChatId, targetChatId)
                .map(states -> {
                    String status;
                    if (!item.getBoolean("enabled", false)) {
                        status = "PAUSED";
                    } else if (states.isEmpty()) {
                        status = "INITIALIZING";
                    } else if (states.stream().anyMatch(state -> "ERROR".equals(state.status()))) {
                        status = "ERROR";
                    } else if (states.stream().anyMatch(state ->
                            "RECOVERING".equals(state.status()))) {
                        status = "RECOVERING";
                    } else {
                        status = "LIVE";
                    }
                    item.put("syncStatus", status)
                            .put("syncTopicCount", states.size())
                            .put("syncScannedCount", states.stream()
                                    .mapToInt(CloudArchiveSyncState::scannedCount).sum())
                            .put("syncMatchedCount", states.stream()
                                    .mapToInt(CloudArchiveSyncState::matchedCount).sum())
                            .put("syncQueuedCount", states.stream()
                                    .mapToInt(CloudArchiveSyncState::queuedCount).sum())
                            .put("syncLastObservedMessageId", states.stream()
                                    .mapToLong(CloudArchiveSyncState::lastObservedMessageId)
                                    .max().orElse(0L))
                            .put("syncRecoveryTargetMessageId", states.stream()
                                    .mapToLong(CloudArchiveSyncState::recoveryTargetMessageId)
                                    .max().orElse(0L))
                            .put("syncRecoveryCursorMessageId", states.stream()
                                    .mapToLong(CloudArchiveSyncState::recoveryCursorMessageId)
                                    .max().orElse(0L))
                            .put("lastReconciledAt", states.stream()
                                    .mapToLong(CloudArchiveSyncState::lastReconciledAt)
                                    .max().orElse(0L));
                    states.stream().map(CloudArchiveSyncState::lastError)
                            .filter(StrUtil::isNotBlank).findFirst()
                            .ifPresent(error -> item.put("syncError", error));
                    return item;
                })
                .recover(failure -> Future.succeededFuture(item
                        .put("syncStatus", "ERROR")
                        .put("syncError", StrUtil.blankToDefault(
                                failure.getMessage(), failure.getClass().getSimpleName()))));
    }

    private void handleCloudArchiveRecords(RoutingContext ctx) {
        int limit = Math.max(1, Math.min(Convert.toInt(ctx.queryParams().get("limit"), 100), 200));
        DataVerticle.cloudArchiveRepository.listRecent(limit)
                .compose(records -> {
                    List<Future<JsonObject>> items = records.stream()
                            .map(this::cloudArchiveRecordJson)
                            .toList();
                    return Future.all(items);
                })
                .map(records -> {
                    List<JsonObject> items = new ArrayList<>(records.size());
                    for (int index = 0; index < records.size(); index++) {
                        items.add(records.resultAt(index));
                    }
                    return new JsonArray(items);
                })
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private Future<JsonObject> cloudArchiveRecordJson(CloudArchiveRecord record) {
        JsonObject item = JsonObject.mapFrom(record);
        TelegramVerticles.get(record.telegramId()).ifPresent(telegram -> {
            TdApi.Chat source = telegram.getChat(record.sourceChatId());
            TdApi.Chat target = telegram.getChat(record.targetChatId());
            item.put("sourceChatName", source == null ? Convert.toStr(record.sourceChatId()) : source.title)
                    .put("targetChatName", target == null ? Convert.toStr(record.targetChatId()) : target.title);
        });
        item
                .put("sourceChatName", item.getString("sourceChatName", Convert.toStr(record.sourceChatId())))
                .put("targetChatName", item.getString("targetChatName", Convert.toStr(record.targetChatId())));
        if (record.sourceTopicId() == 0 || record.targetTopicId() == 0) {
            return Future.succeededFuture(item);
        }
        return DataVerticle.cloudArchiveTopicRepository.find(
                        record.telegramId(), record.sourceChatId(), record.sourceTopicId(),
                        record.targetChatId())
                .map(mapping -> {
                    if (mapping != null && mapping.targetTopicId() == record.targetTopicId()) {
                        item.put("sourceTopicName", mapping.sourceTopicName())
                                .put("targetTopicName", mapping.targetTopicName())
                                .put("generalTopic", mapping.general());
                    }
                    return item;
                });
    }

    private void handleCloudArchiveHistory(RoutingContext ctx) {
        int limit = Math.max(1, Math.min(Convert.toInt(ctx.queryParams().get("limit"), 50), 200));
        DataVerticle.cloudArchiveHistoryRepository.listRecent(limit)
                .map(jobs -> new JsonArray(jobs.stream().map(this::cloudArchiveHistoryJson).toList()))
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private JsonObject cloudArchiveHistoryJson(CloudArchiveHistoryJob job) {
        JsonObject item = JsonObject.mapFrom(job);
        item.remove("ruleJson");
        try {
            JsonObject savedRule = new JsonObject(job.ruleJson());
            item.put("topicMode", savedRule.getString("topicMode", "MERGE"))
                    .put("archiveMode", savedRule.getString("mode", "COPY"));
        } catch (RuntimeException ignored) {
            item.put("topicMode", "UNKNOWN").put("archiveMode", "UNKNOWN");
        }
        TelegramVerticles.get(job.telegramId()).ifPresent(telegram -> {
            TdApi.Chat source = telegram.getChat(job.sourceChatId());
            TdApi.Chat target = telegram.getChat(job.targetChatId());
            item.put("sourceChatName", source == null ? Convert.toStr(job.sourceChatId()) : source.title)
                    .put("targetChatName", target == null ? Convert.toStr(job.targetChatId()) : target.title);
        });
        return item
                .put("dailyLimit", job.dailyLimit())
                .put("dailyDate", job.dailyDate())
                .put("dailyForwardedCount", job.dailyForwardedCount())
                .put("sourceChatName", item.getString("sourceChatName", Convert.toStr(job.sourceChatId())))
                .put("targetChatName", item.getString("targetChatName", Convert.toStr(job.targetChatId())));
    }

    private void handleCloudArchiveHistoryCreate(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null) {
            ctx.fail(400);
            return;
        }
        long telegramId = Convert.toLong(body.getValue("telegramId"));
        long sourceChatId = Convert.toLong(body.getValue("sourceChatId"));
        String scanMode = "ALL".equalsIgnoreCase(body.getString("scanMode")) ? "ALL" : "LIMIT";
        int maxMessages = "ALL".equals(scanMode) ? 0
                : Math.max(1, Math.min(Convert.toInt(body.getValue("maxMessages"), 1000), 100_000));
        int dailyLimit = Math.max(0, Convert.toInt(body.getValue("dailyLimit"), 500));
        SettingAutoRecords.Automation automation = AutomationsHolder.INSTANCE.autoRecords()
                .getItem(telegramId, sourceChatId);
        if (automation == null || automation.archive == null || !automation.archive.enabled
            || automation.archive.rule == null || automation.archive.rule.targetChatId == 0) {
            ctx.response().setStatusCode(400).end(JsonObject.of(
                    "error", "Save and enable the cloud archive rule before starting history"
            ).encode());
            return;
        }
        SettingAutoRecords.ArchiveRule rule = automation.archive.rule;
        DataVerticle.cloudArchiveHistoryRepository.create(
                        telegramId,
                        sourceChatId,
                        rule.sourceTopicId,
                        rule.targetChatId,
                        rule.targetTopicId,
                        Json.encode(rule),
                        scanMode,
                        maxMessages,
                        dailyLimit)
                .map(this::cloudArchiveHistoryJson)
                .onSuccess(ctx::json)
                .onFailure(failure -> {
                    if (failure instanceof IllegalStateException) {
                        ctx.response().setStatusCode(409)
                                .end(JsonObject.of("error", failure.getMessage()).encode());
                    } else {
                        ctx.fail(failure);
                    }
                });
    }

    private void handleCloudArchiveHistoryDailyLimit(RoutingContext ctx) {
        String jobId = ctx.pathParam("jobId");
        JsonObject body = ctx.body().asJsonObject();
        if (StrUtil.isBlank(jobId) || body == null) {
            ctx.fail(400);
            return;
        }
        int dailyLimit = Math.max(0, Convert.toInt(body.getValue("dailyLimit"), 500));
        DataVerticle.cloudArchiveHistoryRepository.updateDailyLimit(jobId, dailyLimit)
                .onSuccess(_ -> {
                    AutoCloudArchiveVerticle.clearJobQuotaExhausted(jobId);
                    ctx.json(JsonObject.of("updated", true, "dailyLimit", dailyLimit));
                })
                .onFailure(ctx::fail);
    }

    private void handleCloudArchiveHistoryAction(RoutingContext ctx) {
        String jobId = ctx.pathParam("jobId");
        String action = ctx.pathParam("action");
        if (StrUtil.isBlank(jobId) || !Set.of("pause", "resume", "cancel", "delete").contains(action)) {
            ctx.fail(400);
            return;
        }
        if ("resume".equals(action)) {
            AutoCloudArchiveVerticle.clearJobQuotaExhausted(jobId);
        }
        Future<Boolean> transition = Set.of("cancel", "delete").contains(action)
                ? DataVerticle.cloudArchiveRepository.cancelHistory(jobId)
                        .compose(_ -> DataVerticle.cloudArchiveRepository.releaseHistory(
                                AutoCloudArchiveVerticle.liveHistoryKey(jobId)))
                        .compose(_ -> DataVerticle.cloudArchiveHistoryRepository.transition(jobId, action))
                : DataVerticle.cloudArchiveHistoryRepository.transition(jobId, action);
        if ("pause".equals(action)) {
            transition = transition.compose(updated -> updated
                    ? DataVerticle.cloudArchiveRepository.releaseHistory(
                            AutoCloudArchiveVerticle.liveHistoryKey(jobId)).map(true)
                    : Future.succeededFuture(false));
        }
        transition
                .onSuccess(updated -> {
                    if (!updated) {
                        ctx.fail(409);
                    } else {
                        ctx.json(JsonObject.of("updated", true));
                    }
                })
                .onFailure(ctx::fail);
    }

    private void handleCloudArchiveValidate(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null) {
            ctx.fail(400);
            return;
        }
        TelegramVerticle telegram = TelegramVerticles.getOrElseThrow(body.getString("telegramId"));
        long sourceChatId = Convert.toLong(body.getValue("sourceChatId"));
        SettingAutoRecords.ArchiveRule rule = body.getJsonObject("rule", new JsonObject())
                .mapTo(SettingAutoRecords.ArchiveRule.class);
        CloudArchiveService.validate(telegram, sourceChatId, rule)
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleCloudArchiveTest(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null) {
            ctx.fail(400);
            return;
        }
        TelegramVerticle telegram = TelegramVerticles.getOrElseThrow(body.getString("telegramId"));
        long telegramId = Convert.toLong(body.getValue("telegramId"));
        long sourceChatId = Convert.toLong(body.getValue("sourceChatId"));
        SettingAutoRecords.ArchiveRule rule = body.getJsonObject("rule", new JsonObject())
                .mapTo(SettingAutoRecords.ArchiveRule.class);
        long now = System.currentTimeMillis();
        long lastTest = cloudArchiveTestTimestamps.getOrDefault(telegramId, 0L);
        if (now - lastTest < Duration.ofMinutes(1).toMillis()) {
            long retryAfter = Math.max(1, (Duration.ofMinutes(1).toMillis() - (now - lastTest)) / 1000);
            ctx.response().setStatusCode(429)
                    .putHeader("Retry-After", Long.toString(retryAfter))
                    .end(JsonObject.of("error", "Wait before sending another real test").encode());
            return;
        }
        cloudArchiveTestTimestamps.put(telegramId, now);
        latestArchiveTestMessage(telegram, sourceChatId, rule.sourceTopicId)
                .compose(messages -> {
                    TdApi.Message source = messages == null || messages.messages == null || messages.messages.length == 0
                            ? null : messages.messages[0];
                    if (source == null) {
                        return Future.failedFuture("The source chat has no message to test");
                    }
                    SettingAutoRecords.ArchiveTopicMode topicMode = rule.topicMode == null
                            ? SettingAutoRecords.ArchiveTopicMode.MERGE : rule.topicMode;
                    if (topicMode != SettingAutoRecords.ArchiveTopicMode.PRESERVE) {
                        return CloudArchiveService.archive(telegram, sourceChatId, List.of(source.id), rule);
                    }
                    if (!(source.topicId instanceof TdApi.MessageTopicForum topic)) {
                        return Future.failedFuture("The test message doesn't belong to a forum topic");
                    }
                    return CloudArchiveTopicService.resolve(
                                    telegram, sourceChatId, topic.forumTopicId, rule.targetChatId)
                            .compose(targetTopicId -> {
                                rule.targetTopicId = targetTopicId;
                                rule.topicMode = SettingAutoRecords.ArchiveTopicMode.MERGE;
                                return CloudArchiveService.archive(
                                        telegram, sourceChatId, List.of(source.id), rule);
                            });
                })
                .map(targets -> new JsonObject().put("targetMessageIds", new JsonArray(targets.values().stream().toList())))
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleCloudArchiveRetry(RoutingContext ctx) {
        String recordId = ctx.pathParam("recordId");
        if (StrUtil.isBlank(recordId)) {
            ctx.fail(400);
            return;
        }
        AutoCloudArchiveVerticle.QUEUED_RECORD_AT.remove(recordId);
        DataVerticle.cloudArchiveRepository.retry(recordId)
                .onSuccess(updated -> {
                    if (!updated) {
                        ctx.fail(404);
                    } else {
                        ctx.json(JsonObject.of("queued", true));
                    }
                })
                .onFailure(ctx::fail);
    }

    private void handleCloudArchiveRetryAll(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        long telegramId = body != null ? Convert.toLong(body.getValue("telegramId"), 0L) : 0L;
        Long chatId = body != null && body.getValue("chatId") != null ? Convert.toLong(body.getValue("chatId")) : null;
        AutoCloudArchiveVerticle.QUEUED_RECORD_AT.clear();
        DataVerticle.cloudArchiveRepository.retryAll(telegramId, chatId)
                .onSuccess(count -> ctx.json(JsonObject.of("count", count, "queued", true)))
                .onFailure(ctx::fail);
    }

    private void handleCloudArchiveClearRecords(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        long telegramId = body != null ? Convert.toLong(body.getValue("telegramId"), 0L) : 0L;
        String status = body != null ? body.getString("status", "ALL") : "ALL";
        DataVerticle.cloudArchiveRepository.clearRecords(telegramId, status)
                .onSuccess(count -> ctx.json(JsonObject.of("count", count)))
                .onFailure(ctx::fail);
    }

    private void handleLocalOrganizeOverview(RoutingContext ctx) {
        List<JsonObject> rules = AutomationsHolder.INSTANCE.autoRecords().getTransferConfiguredItems().stream()
                .map(automation -> {
                    JsonObject item = new JsonObject()
                            .put("telegramId", Convert.toStr(automation.telegramId))
                            .put("sourceChatId", Convert.toStr(automation.chatId))
                            .put("enabled", automation.transfer.enabled)
                            .put("rule", automation.transfer.rule);
                    TelegramVerticles.get(automation.telegramId).ifPresent(telegram -> {
                        item.put("accountName", accountDisplayName(telegram));
                        TdApi.Chat source = telegram.getChat(automation.chatId);
                        item.put("sourceChatName", source == null ? Convert.toStr(automation.chatId) : source.title);
                    });
                    return item
                            .put("accountName", item.getString("accountName", item.getString("telegramId")))
                            .put("sourceChatName", item.getString("sourceChatName", item.getString("sourceChatId")));
                })
                .sorted(Comparator.comparing(item -> item.getString("sourceChatName", "")))
                .toList();
        ctx.json(new JsonObject().put("rules", new JsonArray(rules)));
    }

    private void handleTelegramTopics(RoutingContext ctx) {
        TelegramVerticle telegram = getTelegramVerticleByPath(ctx);
        if (telegram == null) {
            return;
        }
        long chatId = Convert.toLong(ctx.pathParam("chatId"));
        String query = StrUtil.blankToDefault(ctx.request().getParam("query"), "");
        if (chatId == 0) {
            ctx.fail(400);
            return;
        }
        if (!telegram.isForum(chatId)) {
            ctx.json(new JsonArray());
            return;
        }
        TelegramTopics.listAll(telegram.client, chatId, query)
                .map(result -> new JsonArray(result.stream()
                        .filter(topic -> topic != null && topic.info != null)
                        .map(topic -> new JsonObject()
                                .put("id", Integer.toString(topic.info.forumTopicId))
                                .put("name", topic.info.name)
                                .put("general", topic.info.isGeneral)
                                .put("closed", topic.info.isClosed)
                                .put("hidden", topic.info.isHidden))
                        .toList()))
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private Future<TdApi.Messages> latestArchiveTestMessage(TelegramVerticle telegram,
                                                             long sourceChatId,
                                                             long sourceTopicId) {
        if (sourceTopicId == 0) {
            return telegram.client.execute(new TdApi.GetChatHistory(sourceChatId, 0, 0, 1, false));
        }
        return telegram.client.execute(new TdApi.GetForumTopicHistory(
                sourceChatId, Math.toIntExact(sourceTopicId), 0, 0, 1));
    }

    private void handleLocalOrganizeSources(RoutingContext ctx) {
        long telegramId = Convert.toLong(ctx.queryParams().get("telegramId"));
        if (telegramId == 0 || TelegramVerticles.get(telegramId).isEmpty()) {
            ctx.fail(400);
            return;
        }
        boolean eligibleOnly = Convert.toBool(ctx.queryParams().get("eligibleOnly"), true);
        String query = StrUtil.blankToDefault(URLUtil.decode(ctx.queryParams().get("query")), "")
                .toLowerCase(Locale.ROOT);
        DataVerticle.fileRepository.listLocalSources(telegramId, eligibleOnly)
                .map(sources -> {
                    TelegramVerticle telegram = TelegramVerticles.get(telegramId).orElse(null);
                    return sources.stream().map(source -> {
                        long chatId = Convert.toLong(source.getString("chatId"));
                        TdApi.Chat chat = telegram == null ? null : telegram.getChat(chatId);
                        String name = chat == null ? Long.toString(chatId) : chat.title;
                        return source.copy()
                                .put("id", Long.toString(chatId))
                                .put("name", name)
                                .put("type", chat == null ? "unknown" : TdApiHelp.getChatType(chat.type));
                    }).filter(source -> query.isBlank()
                            || source.getString("name", "").toLowerCase(Locale.ROOT).contains(query)
                            || source.getString("chatId", "").contains(query))
                            .toList();
                })
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleLocalOrganizePreview(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null) {
            ctx.fail(400);
            return;
        }
        long telegramId = Convert.toLong(body.getValue("telegramId"));
        long chatId = Convert.toLong(body.getValue("sourceChatId"));
        SettingAutoRecords.TransferRule rule = body.getJsonObject("rule", new JsonObject())
                .mapTo(SettingAutoRecords.TransferRule.class);
        if (telegramId == 0 || chatId == 0 || StrUtil.isBlank(rule.destination) || rule.transferPolicy == null
            || rule.duplicationPolicy == null) {
            ctx.fail(400);
            return;
        }
        Transfer transfer = Transfer.create(rule);
        Map<String, String> filters = new HashMap<>();
        filters.put("telegramId", Long.toString(telegramId));
        filters.put("downloadStatus", "completed");
        filters.put("transferStatus", "idle");
        filters.put("limit", "20");
        if (rule.sourceTopicId != 0) {
            filters.put("messageThreadId", Long.toString(rule.sourceTopicId));
        }
        DataVerticle.fileRepository.getFiles(chatId, filters)
                .map(result -> new JsonObject()
                        .put("total", result.v3)
                        .put("items", new JsonArray(result.v1.stream()
                                .filter(record -> !"thumbnail".equals(record.type()))
                                .filter(record -> StrUtil.isNotBlank(record.localPath()))
                                .map(record -> {
                                    String destination = transfer.previewPath(record);
                                    return new JsonObject()
                                            .put("uniqueId", record.uniqueId())
                                            .put("fileName", record.fileName())
                                            .put("sourcePath", record.localPath())
                                            .put("destinationPath", destination)
                                            .put("destinationExists", FileUtil.exist(destination));
                                })
                                .toList())))
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private String accountDisplayName(TelegramVerticle telegramVerticle) {
        if (telegramVerticle.telegramRecord == null) {
            return Convert.toStr(telegramVerticle.getId());
        }
        return StrUtil.blankToDefault(telegramVerticle.telegramRecord.firstName(), Convert.toStr(telegramVerticle.getId()));
    }

    private String minithumbnail(TdApi.Chat chat) {
        byte[] data = (byte[]) BeanUtil.getProperty(chat, "photo.minithumbnail.data");
        return data == null ? "" : Base64.encode(data);
    }

    private void handleTelegramFiles(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = getTelegramVerticleByPath(ctx);
        if (telegramVerticle == null) {
            return;
        }
        String chatId = ctx.pathParam("chatId");
        if (StrUtil.isBlank(chatId)) {
            ctx.fail(400);
            return;
        }
        String link = URLUtil.decode(ctx.queryParams().get("link"));
        if (StrUtil.isNotBlank(link)) {
            telegramVerticle.parseLink(link)
                    .onSuccess(ctx::json)
                    .onFailure(ctx::fail);
            return;
        }

        Map<String, String> filter = new HashMap<>();
        ctx.request().params().forEach(filter::put);
        filter.put("search", URLUtil.decode(filter.get("search")));

        telegramVerticle.getChatFiles(Convert.toLong(chatId), filter)
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleTelegramFilesCount(RoutingContext ctx) {
        boolean offline = Convert.toBool(ctx.queryParams().get("offline"), false);
        Long telegramId = Convert.toLong(ctx.pathParam("telegramId"), -1L);
        Long chatId = Convert.toLong(ctx.pathParam("chatId"), -1L);
        if (offline) {
            if (Convert.toBool(ctx.queryParams().get("seedOnly"), false)) {
                Map<String, String> filter = new HashMap<>();
                ctx.queryParams().forEach(entry -> filter.put(entry.getKey(), entry.getValue()));
                DataVerticle.torrentRepository.countSeedOnlyWithType(filter)
                        .onSuccess(ctx::json)
                        .onFailure(ctx::fail);
                return;
            }
            DataVerticle.fileRepository.countWithType(telegramId, chatId)
                    .onSuccess(ctx::json)
                    .onFailure(ctx::fail);
            return;
        }

        TelegramVerticle telegramVerticle = getTelegramVerticleByPath(ctx);
        if (telegramVerticle == null) {
            return;
        }
        telegramVerticle.getChatFilesCount(chatId)
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleTelegramDownloadStatistics(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = getTelegramVerticleByPath(ctx);
        if (telegramVerticle == null) {
            return;
        }

        String type = ctx.request().getParam("type");
        String timeRange = ctx.request().getParam("timeRange");
        (Objects.equals(type, "phase") ? telegramVerticle.getDownloadStatisticsByPhase(Convert.toInt(timeRange, 1)) :
                telegramVerticle.getDownloadStatistics())
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleTelegramChange(RoutingContext ctx) {
        String sessionId = ctx.session().id();
        String telegramId = ctx.request().getParam("telegramId");
        if (handleTelegramChange(sessionId, telegramId)) {
            ctx.end();
        } else {
            ctx.fail(400);
        }
    }

    private boolean handleTelegramChange(String sessionId, String telegramId) {
        TelegramVerticle previous = sessionTelegramVerticles.get(sessionId);
        if (StrUtil.isBlank(telegramId)) {
            sessionTelegramVerticles.remove(sessionId);
            if (previous != null) previous.releaseFrontendSession(sessionId);
            return true;
        }
        Optional<TelegramVerticle> optionalTelegramVerticle = TelegramVerticles.get(telegramId);
        if (optionalTelegramVerticle.isEmpty()) {
            return false;
        }
        TelegramVerticle selected = optionalTelegramVerticle.get();
        sessionTelegramVerticles.put(sessionId, selected);
        if (previous != null && previous != selected) previous.releaseFrontendSession(sessionId);
        if (clients.containsKey(sessionId)) selected.retainFrontendSession(sessionId);
        return true;
    }

    private void handleTelegramToggleProxy(RoutingContext ctx) {
        String telegramId = ctx.request().getParam("telegramId");
        TelegramVerticles.get(telegramId)
                .ifPresentOrElse(telegramVerticle ->
                        telegramVerticle.toggleProxy(ctx.body().asJsonObject())
                                .onSuccess(r -> ctx.json(JsonObject.of("proxy", r)))
                                .onFailure(ctx::fail), () -> ctx.fail(404));
    }

    private void handleTelegramPing(RoutingContext ctx) {
        String telegramId = ctx.pathParam("telegramId");
        if (StrUtil.isBlank(telegramId)) {
            ctx.fail(400);
            return;
        }
        TelegramVerticles.get(telegramId)
                .ifPresentOrElse(telegramVerticle ->
                        telegramVerticle.ping()
                                .onSuccess(r -> ctx.json(JsonObject.of("ping", r)))
                                .onFailure(ctx::fail), () -> ctx.fail(404)
                );
    }

    private void handleTelegramTestNetwork(RoutingContext ctx) {
        String telegramId = ctx.pathParam("telegramId");
        if (StrUtil.isBlank(telegramId)) {
            ctx.fail(400);
            return;
        }
        TelegramVerticles.get(telegramId)
                .ifPresentOrElse(telegramVerticle ->
                                telegramVerticle.client.execute(new TdApi.TestNetwork(), 10000, vertx)
                                        .onComplete(r ->
                                                ctx.json(JsonObject.of("success", r.succeeded()))),
                        () -> ctx.fail(404)
                );
    }

    private void handleTelegramApiMethods(RoutingContext ctx) {
        Map<String, Class<TdApi.Function<?>>> functions = TdApiHelp.getFunctions();
        ctx.json(JsonObject.of("methods", functions.keySet()));
    }

    private void handleTelegramApiMethodParameters(RoutingContext ctx) {
        String method = ctx.pathParam("method");
        ctx.json(JsonObject.of("parameters", TdApiHelp.getFunction(method, null)));
    }

    private void handleTelegramApi(RoutingContext ctx) {
        String method = ctx.pathParam("method");
        if (method == null) {
            ctx.fail(400);
            return;
        }
        TelegramVerticle telegramVerticle = getTelegramVerticleBySession(ctx);
        if (telegramVerticle == null) {
            return;
        }
        JsonObject params = ctx.body().asJsonObject();
        telegramVerticle.execute(method, params == null ? null : params.getMap())
                .onSuccess(code -> ctx.json(JsonObject.of("code", code)))
                .onFailure(ctx::fail);
    }

    private void handleFilePreview(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = getTelegramVerticleByPath(ctx);
        if (telegramVerticle == null) {
            return;
        }
        String uniqueId = ctx.pathParam("uniqueId");
        if (StrUtil.isBlank(uniqueId)) {
            ctx.fail(404);
            return;
        }

        telegramVerticle.resolveMediaTarget(uniqueId)
                .onSuccess(target -> {
                    String mimeType = target.mimeType();
                    if (StrUtil.isBlank(mimeType)) {
                        mimeType = "video".equals(target.type()) ? "video/mp4" : "application/octet-stream";
                    }

                    if (target.isCompleted() && StrUtil.isNotBlank(target.localPath()) && FileUtil.exist(target.localPath())) {
                        try {
                            Path allowedFile = resolveAllowedFile(target.localPath());
                            fileRouteHandler.handle(ctx, allowedFile.toString(), mimeType);
                        } catch (IllegalArgumentException exception) {
                            respondJson(ctx, 403, "FILE_PATH_NOT_ALLOWED", "File is outside an allowed root");
                        }
                        return;
                    }

                    // If thumbnail or photo, fetch on-demand
                    if ("thumbnail".equals(target.type()) || "photo".equals(target.type())) {
                        telegramVerticle.fetchMediaPreview(target.fileId(), mimeType, uniqueId)
                                .onSuccess(tuple -> {
                                    try {
                                        Path allowedFile = resolveAllowedFile(tuple.v1);
                                        fileRouteHandler.handle(ctx, allowedFile.toString(), tuple.v2);
                                    } catch (IllegalArgumentException exception) {
                                        respondJson(ctx, 403, "FILE_PATH_NOT_ALLOWED", "File is outside an allowed root");
                                    }
                                })
                                .onFailure(ctx::fail);
                        return;
                    }

                    // For video or audio, handle Range streaming via TDLib
                    handleMediaStream(ctx, telegramVerticle, target, mimeType);
                })
                .onFailure(ctx::fail);
    }

    private void handleMediaStream(RoutingContext ctx, TelegramVerticle tv, TelegramVerticle.MediaTarget target, String mimeType) {
        HttpServerRequest request = ctx.request();
        HttpServerResponse response = ctx.response();
        if (response.closed()) return;

        long totalSize = target.size();
        String rangeHeader = request.getHeader("Range");

        long start = 0;
        long end = totalSize > 0 ? totalSize - 1 : 1024 * 1024;
        boolean isRange = false;

        if (StrUtil.isNotBlank(rangeHeader)) {
            Matcher matcher = Pattern.compile("^bytes=(\\d*)-(\\d*)$").matcher(rangeHeader);
            if (matcher.matches()) {
                isRange = true;
                String startPart = matcher.group(1);
                String endPart = matcher.group(2);
                if (StrUtil.isNotBlank(startPart)) {
                    start = Convert.toLong(startPart, 0L);
                }
                if (StrUtil.isNotBlank(endPart)) {
                    end = Convert.toLong(endPart, totalSize > 0 ? totalSize - 1 : start + 1024 * 1024);
                } else if (totalSize > 0) {
                    end = totalSize - 1;
                }
            }
        }

        if (totalSize > 0 && start >= totalSize) {
            response.setStatusCode(416)
                    .putHeader("Content-Range", "bytes */" + totalSize)
                    .end();
            return;
        }

        long maxChunk = 1024 * 1024;
        long count = Math.min(end - start + 1, maxChunk);
        long actualEnd = start + count - 1;

        response.putHeader("Accept-Ranges", "bytes");
        response.putHeader("Content-Type", mimeType);

        long finalStart = start;
        long finalActualEnd = actualEnd;
        boolean finalIsRange = isRange;

        tv.readMediaChunk(target.fileId(), start, count)
                .onSuccess(bytes -> {
                    if (response.closed()) return;
                    if (finalIsRange || totalSize > 0) {
                        response.setStatusCode(206);
                        String contentRange = "bytes " + finalStart + "-" + (finalStart + bytes.length - 1) + "/" + (totalSize > 0 ? totalSize : "*");
                        response.putHeader("Content-Range", contentRange);
                    }
                    response.putHeader("Content-Length", String.valueOf(bytes.length));
                    response.end(Buffer.buffer(bytes));
                })
                .onFailure(err -> {
                    if (!response.closed()) {
                        log.warn("Media streaming chunk error for fileId={}: {}", target.fileId(), err.getMessage());
                        response.setStatusCode(500).end(JsonObject.of("error", err.getMessage()).encode());
                    }
                });
    }

    private static Path resolveAllowedFile(String rawPath) {
        if (StrUtil.isBlank(rawPath)) {
            throw new IllegalArgumentException("File path is missing");
        }
        List<Path> roots = new ArrayList<>();
        roots.add(Path.of(Config.TELEGRAM_ROOT));
        Path sharedRoot = Config.shareConfiguration().sharedRoot();
        if (Files.isDirectory(sharedRoot)) {
            roots.add(sharedRoot);
        }
        return new SafePathResolver(roots).requireAllowedRegularFile(Path.of(rawPath));
    }

    private void handleFileStartDownload(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(ctx.pathParam("telegramId"));

        JsonObject jsonObject = ctx.body().asJsonObject();
        Long chatId = jsonObject.getLong("chatId");
        Long messageId = jsonObject.getLong("messageId");
        Integer fileId = jsonObject.getInteger("fileId");
        if (chatId == null || messageId == null || fileId == null) {
            ctx.fail(400);
            return;
        }

        telegramVerticle.startDownload(chatId, messageId, fileId)
                .onSuccess(ctx::json).onFailure(ctx::fail);
    }

    private void handleFileCancelDownload(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(ctx.pathParam("telegramId"));

        JsonObject jsonObject = ctx.body().asJsonObject();
        Integer fileId = jsonObject.getInteger("fileId");
        if (fileId == null) {
            ctx.fail(400);
            return;
        }

        telegramVerticle.cancelDownload(fileId).onSuccess(_ -> ctx.end()).onFailure(ctx::fail);
    }

    private void handleFileTogglePauseDownload(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(ctx.pathParam("telegramId"));

        JsonObject jsonObject = ctx.body().asJsonObject();
        Integer fileId = jsonObject.getInteger("fileId");
        Boolean isPaused = jsonObject.getBoolean("isPaused");
        if (fileId == null || isPaused == null) {
            ctx.fail(400);
            return;
        }

        telegramVerticle.togglePauseDownload(fileId, isPaused)
                .onSuccess(_ -> ctx.end()).onFailure(ctx::fail);
    }

    private void handleFileRemove(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(ctx.pathParam("telegramId"));

        JsonObject jsonObject = ctx.body().asJsonObject();
        Integer fileId = jsonObject.getInteger("fileId");
        String uniqueId = jsonObject.getString("uniqueId");
        if (fileId == null && StrUtil.isBlank(uniqueId)) {
            ctx.fail(400);
            return;
        }

        telegramVerticle.removeFile(fileId, uniqueId)
                .onSuccess(_ -> ctx.end())
                .onFailure(ctx::fail);
    }

    private void handleFileStartDownloadMultiple(RoutingContext ctx) {
        handleFileControlMultiple(ctx, (telegramVerticle, file) -> {
            Long chatId = file.getLong("chatId");
            Long messageId = file.getLong("messageId");
            Integer fileId = file.getInteger("fileId");
            if (chatId == null || messageId == null || fileId == null) {
                return Future.failedFuture("Invalid parameters");
            }
            return telegramVerticle.startDownload(chatId, messageId, fileId);
        }, null);
    }

    private void handleFileCancelDownloadMultiple(RoutingContext ctx) {
        handleFileControlMultiple(ctx, (telegramVerticle, file) -> {
            Integer fileId = file.getInteger("fileId");
            if (fileId == null) {
                return Future.failedFuture("Invalid parameters");
            }
            return telegramVerticle.cancelDownload(fileId);
        }, "CANCEL_V1");
    }

    private void handleFileTogglePauseDownloadMultiple(RoutingContext ctx) {
        JsonObject jsonObject = ctx.body().asJsonObject();
        Boolean isPaused = jsonObject.getBoolean("isPaused");
        if (isPaused == null) {
            ctx.fail(400);
            return;
        }

        handleFileControlMultiple(ctx, (telegramVerticle, file) -> {
            Integer fileId = file.getInteger("fileId");
            if (fileId == null) {
                return Future.failedFuture("Invalid parameters");
            }
            return telegramVerticle.togglePauseDownload(fileId, isPaused);
        }, isPaused ? "PAUSE_V1" : "RESUME_V1");
    }

    private void handleFileSetUploadLimitMultiple(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        Long uploadLimitBytesPerSecond = Convert.toLong(
                body == null ? null : body.getValue("uploadLimitBytesPerSecond")
        );
        if (uploadLimitBytesPerSecond == null || uploadLimitBytesPerSecond < 0) {
            ctx.fail(400);
            return;
        }
        handleFileControlMultiple(
                ctx,
                (_, _) -> Future.failedFuture("Upload limit is only supported for seed resources"),
                "SET_UPLOAD_LIMIT_V1",
                uploadLimitBytesPerSecond
        );
    }

    private void handleFileRemoveMultiple(RoutingContext ctx) {
        handleFileMultiple(ctx, (telegramVerticle, file) -> {
            Integer fileId = file.getInteger("fileId");
            String uniqueId = file.getString("uniqueId");
            if (fileId == null && StrUtil.isBlank(uniqueId)) {
                return Future.failedFuture("Invalid parameters");
            }
            return telegramVerticle.removeFile(fileId, uniqueId);
        });
    }

    private void handleFileTagsUpdateMultiple(RoutingContext ctx) {
        JsonObject jsonObject = ctx.body().asJsonObject();
        String tags = jsonObject.getString("tags");
        if (StrUtil.isBlank(tags)) {
            ctx.fail(400);
            return;
        }
        handleFileMultiple(ctx, (_, file) -> {
            String uniqueId = file.getString("uniqueId");
            if (StrUtil.isBlank(uniqueId)) {
                return Future.failedFuture("Invalid parameters");
            }
            return DataVerticle.fileRepository.updateTags(uniqueId, tags);
        });
    }

    private void handleFileTransferMultiple(RoutingContext ctx) {
        JsonObject jsonObject = ctx.body().asJsonObject();
        if (jsonObject == null) {
            ctx.fail(400);
            return;
        }
        JsonArray files = jsonObject.getJsonArray("files");
        String destination = jsonObject.getString("destination");
        if (CollUtil.isEmpty(files) || StrUtil.isBlank(destination)) {
            ctx.response().setStatusCode(400).end(JsonObject.of("error", "Destination and files are required").encode());
            return;
        }

        String modeStr = jsonObject.getString("transferMode", "MOVE");
        SettingAutoRecords.TransferMode transferMode;
        try {
            transferMode = SettingAutoRecords.TransferMode.valueOf(modeStr);
        } catch (Exception e) {
            transferMode = SettingAutoRecords.TransferMode.MOVE;
        }

        String policyStr = jsonObject.getString("transferPolicy", "DIRECT");
        Transfer.TransferPolicy transferPolicy;
        try {
            transferPolicy = Transfer.TransferPolicy.valueOf(policyStr);
        } catch (Exception e) {
            transferPolicy = Transfer.TransferPolicy.DIRECT;
        }

        String dupStr = jsonObject.getString("duplicationPolicy", "OVERWRITE");
        Transfer.DuplicationPolicy duplicationPolicy;
        try {
            duplicationPolicy = Transfer.DuplicationPolicy.valueOf(dupStr);
        } catch (Exception e) {
            duplicationPolicy = Transfer.DuplicationPolicy.OVERWRITE;
        }

        boolean useCaptionName = jsonObject.getBoolean("useCaptionName", false);

        SettingAutoRecords.TransferRule rule = new SettingAutoRecords.TransferRule();
        rule.destination = destination;
        rule.transferMode = transferMode;
        rule.transferPolicy = transferPolicy;
        rule.duplicationPolicy = duplicationPolicy;
        rule.useCaptionName = useCaptionName;
        rule.extra = jsonObject.getJsonObject("extra", new JsonObject());

        List<String> uniqueIds = files.stream()
                .map(f -> ((JsonObject) f).getString("uniqueId"))
                .filter(StrUtil::isNotBlank)
                .toList();

        if (uniqueIds.isEmpty()) {
            ctx.json(JsonObject.of("transferredCount", 0));
            return;
        }

        Transfer transfer = Transfer.create(rule);
        transfer.transferStatusUpdated = updated -> {
            DataVerticle.fileRepository.updateTransferStatus(
                    updated.fileRecord().uniqueId(),
                    updated.transferStatus(),
                    updated.localPath()
            );
        };

        DataVerticle.fileRepository.getFilesByUniqueId(uniqueIds)
                .onSuccess(recordsMap -> {
                    int count = 0;
                    for (FileRecord record : recordsMap.values()) {
                        if (record.isDownloadStatus(FileRecord.DownloadStatus.completed)
                                && StrUtil.isNotBlank(record.localPath())) {
                            try {
                                transfer.transfer(record);
                                count++;
                            } catch (Exception e) {
                                log.warn("Failed to transfer file {}: {}", record.uniqueId(), e.getMessage());
                            }
                        }
                    }
                    ctx.json(JsonObject.of("transferredCount", count));
                })
                .onFailure(r -> {
                    log.error("Failed to query files for transfer", r);
                    ctx.fail(500);
                });
    }

    private void handleFileMultiple(RoutingContext ctx, Function2<TelegramVerticle, JsonObject, Future<?>> handler) {
        JsonObject jsonObject = ctx.body().asJsonObject();
        JsonArray files = jsonObject.getJsonArray("files");
        if (CollUtil.isEmpty(files)) {
            ctx.fail(400);
            return;
        }
        Map<Long, List<Object>> groupingByTelegramId = files.stream()
                .collect(Collectors.groupingBy(f -> ((JsonObject) f).getLong("telegramId")));

        Future.all(groupingByTelegramId.entrySet()
                        .stream()
                        .flatMap(entry -> {
                            TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(entry.getKey());

                            return entry.getValue().stream()
                                    .map(f -> {
                                        JsonObject file = (JsonObject) f;
                                        return handler.apply(telegramVerticle, file);
                                    });
                        })
                        .toList()
                )
                .onSuccess(ctx::json).onFailure(r -> {
                    log.error(r, "Failed to handle multiple files: %s".formatted(r.getMessage()));
                    ctx.response()
                            .setStatusCode(400)
                            .end(JsonObject.of("error", "Part of the files failed to process: %s".formatted(r.getMessage())).encode());
                });
    }

    private void handleFileControlMultiple(
            RoutingContext ctx,
            Function2<TelegramVerticle, JsonObject, Future<?>> telegramHandler,
            String seedControlType
    ) {
        handleFileControlMultiple(ctx, telegramHandler, seedControlType, 0);
    }

    private void handleFileControlMultiple(
            RoutingContext ctx,
            Function2<TelegramVerticle, JsonObject, Future<?>> telegramHandler,
            String seedControlType,
            long uploadLimitBytesPerSecond
    ) {
        JsonObject body = ctx.body().asJsonObject();
        JsonArray files = body == null ? null : body.getJsonArray("files");
        if (CollUtil.isEmpty(files)) {
            ctx.fail(400);
            return;
        }
        Future.all(files.stream()
                        .filter(JsonObject.class::isInstance)
                        .map(JsonObject.class::cast)
                        .map(file -> {
                            Long telegramId = file.getLong("telegramId");
                            if (telegramId != null && telegramId != 0) {
                                return telegramHandler.apply(TelegramVerticles.getOrElseThrow(telegramId), file);
                            }
                            if (unifiedFileDownloadService == null) {
                                return Future.failedFuture("Seed control is unavailable");
                            }
                            String uniqueId = file.getString("uniqueId", "");
                            if (!uniqueId.startsWith("seed:")) {
                                return Future.failedFuture("Seed resource is invalid");
                            }
                            String resourceId = uniqueId.substring("seed:".length());
                            return seedControlType == null
                                    ? unifiedFileDownloadService.startSeedResource(resourceId)
                                    : unifiedFileDownloadService.controlSeedResource(
                                            resourceId,
                                            seedControlType,
                                            uploadLimitBytesPerSecond
                                    );
                        })
                        .toList())
                .onSuccess(ctx::json)
                .onFailure(failure -> {
                    log.error(failure, "Failed to control multiple files: {}", failure.getMessage());
                    ctx.response().setStatusCode(400).end(JsonObject.of(
                            "error", "Part of the files failed to process: " + failure.getMessage()
                    ).encode());
                });
    }

    private void handleAutoSettingsUpdate(RoutingContext ctx) {
        TelegramVerticle telegramVerticle = TelegramVerticles.getOrElseThrow(ctx.pathParam("telegramId"));

        String chatId = ctx.request().getParam("chatId");
        if (StrUtil.isBlank(chatId)) {
            ctx.fail(400);
            return;
        }
        JsonObject params = ctx.body().asJsonObject();
        telegramVerticle.updateAutoSettings(Convert.toLong(chatId), params)
                .onSuccess(_ -> ctx.end())
                .onFailure(failure -> {
                    if (failure instanceof IllegalArgumentException) {
                        ctx.response().setStatusCode(400)
                                .putHeader("Content-Type", "application/json")
                                .end(JsonObject.of("error", failure.getMessage()).encode());
                    } else {
                        ctx.fail(failure);
                    }
                });
    }

    private void handleFilesCount(RoutingContext ctx) {
        Future.all(
                        DataVerticle.fileRepository.getDownloadStatistics(),
                        DataVerticle.settingRepository.<Integer>getByKey(SettingKey.autoDownloadLimit)
                )
                .onSuccess(result -> {
                    JsonObject statistics = result.resultAt(0);
                    Integer configuredLimit = result.resultAt(1);
                    statistics.put("downloadLimit", configuredLimit == null
                            ? AutoDownloadVerticle.DEFAULT_LIMIT
                            : configuredLimit);
                    ctx.json(statistics);
                })
                .onFailure(ctx::fail);
    }

    private void handleFilesChats(RoutingContext ctx) {
        String telegramIdParam = ctx.request().getParam("telegramId");
        Long telegramId = StrUtil.isNotBlank(telegramIdParam) ? Convert.toLong(telegramIdParam, null) : null;
        DataVerticle.fileRepository.getCachedChats(telegramId)
                .map(list -> {
                    List<JsonObject> result = new ArrayList<>();
                    for (JsonObject item : list) {
                        long tId = Convert.toLong(item.getString("telegramId"), 0L);
                        long cId = Convert.toLong(item.getString("chatId"), 0L);
                        JsonObject chatJson = new JsonObject()
                                .put("id", Convert.toStr(cId))
                                .put("telegramId", Convert.toStr(tId))
                                .put("type", "channel")
                                .put("totalCount", item.getLong("totalCount", 0L))
                                .put("downloadedCount", item.getLong("downloadedCount", 0L))
                                .put("downloadedSize", item.getLong("downloadedSize", 0L));

                        Optional<TelegramVerticle> tvOpt = TelegramVerticles.get(tId);
                        if (tvOpt.isPresent()) {
                            TdApi.Chat chat = tvOpt.get().getChat(cId);
                            if (chat != null) {
                                chatJson.put("name", chat.id == tId ? "Saved Messages" : chat.title)
                                        .put("type", TdApiHelp.getChatType(chat.type))
                                        .put("avatar", minithumbnail(chat))
                                        .put("unreadCount", chat.unreadCount);
                            }
                        }
                        if (!chatJson.containsKey("name") || StrUtil.isBlank(chatJson.getString("name"))) {
                            chatJson.put("name", "Chat " + cId);
                        }
                        result.add(chatJson);
                    }
                    return new JsonArray(result);
                })
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleFiles(RoutingContext ctx) {
        Map<String, String> filter = new HashMap<>();
        ctx.request().params().forEach(filter::put);
        filter.put("search", URLUtil.decode(filter.get("search")));

        FileRecordRetriever.getFiles(0, filter)
                .onSuccess(ctx::json)
                .onFailure(ctx::fail);
    }

    private void handleFileTagsUpdate(RoutingContext ctx) {
        String uniqueId = ctx.pathParam("uniqueId");
        if (StrUtil.isBlank(uniqueId)) {
            ctx.fail(400);
            return;
        }

        JsonObject params = ctx.body().asJsonObject();
        String tags = params.getString("tags");
        DataVerticle.fileRepository.updateTags(uniqueId, tags)
                .onSuccess(_ -> ctx.end())
                .onFailure(ctx::fail);
    }

    private TelegramVerticle getTelegramVerticleBySession(RoutingContext ctx) {
        String sessionId = ctx.session().id();
        TelegramVerticle telegramVerticle = sessionTelegramVerticles.get(sessionId);
        if (telegramVerticle == null) {
            ctx.response().setStatusCode(400)
                    .end(JsonObject.of("error", "Your session not link any telegram!").encode());
            return null;
        }
        return telegramVerticle;
    }

    private TelegramVerticle getTelegramVerticleByPath(RoutingContext ctx) {
        String telegramId = ctx.pathParam("telegramId");
        if (StrUtil.isBlank(telegramId)) {
            ctx.fail(400);
            return null;
        }
        Optional<TelegramVerticle> telegramVerticleOptional = TelegramVerticles.get(telegramId);
        if (telegramVerticleOptional.isEmpty()) {
            ctx.fail(404);
            return null;
        }
        return telegramVerticleOptional.get();
    }
}
