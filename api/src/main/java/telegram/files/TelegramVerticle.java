package telegram.files;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.codec.Base64;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.drinkless.tdlib.TdApi;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import telegram.files.repository.*;
import telegram.files.share.HttpSeedCoordinatorClient;
import telegram.files.share.UnifiedFileDownloadService;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class TelegramVerticle extends AbstractVerticle {

    private static final Log log = LogFactory.get();

    static final long STALE_DOWNLOAD_RETRY_AFTER_MILLIS = 2 * 60 * 1000L;

    public TelegramGateway client;

    private volatile TelegramGateway tdlibClient;

    private final TelegramGatewayFactory gatewayFactory;

    private TelegramChats telegramChats;

    public boolean authorized = false;

    public TdApi.AuthorizationState lastAuthorizationState;

    public String rootPath;

    private String proxyName;

    private String rootId;

    private boolean needDelete = false;

    public TelegramRecord telegramRecord;

    private AvgSpeed avgSpeed = new AvgSpeed();

    private long avgSpeedPersistenceTimerId;

    private long downloadStatusReconciliationTimerId;

    private volatile TdApi.ConnectionState lastConnectionState;

    private final Map<Integer, Long> fileLastEventTimes = new ConcurrentHashMap<>();

    public record OnlineFileInfo(int fileId, String type, String mimeType, long size) {}

    public record MediaTarget(int fileId, String type, String mimeType, long size, String localPath, boolean isCompleted) {}

    public record PendingChunkRead(long offset, long count, Promise<byte[]> promise) {}

    private final Map<String, OnlineFileInfo> onlineFilesCache = new ConcurrentHashMap<>();

    private final Map<Integer, List<Promise<String>>> pendingPreviewPromises = new ConcurrentHashMap<>();

    private final Map<Integer, List<PendingChunkRead>> pendingChunkReads = new ConcurrentHashMap<>();

    private long lastFileDownloadEventTime;

    private final Map<String, Future<Void>> fileStatusUpdateTails = new HashMap<>();

    private UnifiedFileDownloadService unifiedFileDownloadService;

    private enum ClientLifecycle { STARTING, ACTIVE, STOPPING, SLEEPING, CLOSED }

    private final Object lifecycleLock = new Object();
    private final Set<String> frontendSessions = new HashSet<>();
    private ClientLifecycle clientLifecycle = ClientLifecycle.STARTING;
    private Promise<Void> readyPromise = Promise.promise();
    private Promise<Void> closePromise;
    private int activeRequests;
    private int idleTimeoutMinutes = 0;
    private long idleSleepTimerId;
    private boolean idleClose;
    private boolean permanentClose;
    private boolean wakeAfterClose;
    private boolean downloadsActive;

    public TelegramVerticle(String rootPath) {
        this(rootPath, TelegramGatewayFactory.tdlib());
    }

    TelegramVerticle(String rootPath, TelegramGatewayFactory gatewayFactory) {
        this.rootPath = rootPath;
        this.gatewayFactory = Objects.requireNonNull(gatewayFactory, "gatewayFactory");
    }

    public TelegramVerticle(TelegramRecord telegramRecord) {
        this(telegramRecord, TelegramGatewayFactory.tdlib());
    }

    TelegramVerticle(TelegramRecord telegramRecord, TelegramGatewayFactory gatewayFactory) {
        this.telegramRecord = telegramRecord;
        this.rootPath = telegramRecord.rootPath();
        this.proxyName = telegramRecord.proxy();
        this.gatewayFactory = Objects.requireNonNull(gatewayFactory, "gatewayFactory");
    }

    TelegramVerticle withUnifiedFileDownloadService(UnifiedFileDownloadService service) {
        this.unifiedFileDownloadService = service;
        return this;
    }

    public String getRootId() {
        if (StrUtil.isNotBlank(this.rootId)) return rootId;

        this.rootId = StrUtil.subAfter(this.rootPath, '-', true);
        return this.rootId;
    }

    public Object getId() {
        return telegramRecord == null ? this.getRootId() : telegramRecord.id();
    }

    public void setProxy(String proxyName) {
        this.proxyName = proxyName;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        initializeTelegramGateway();
        Future.all(initEventConsumer(), initAvgSpeed(), initIdleSleep())
                .compose(_ -> this.enableProxy(this.proxyName, tdlibClient))
                .compose(_ -> this.initDownloadStatusReconciliation())
                .onSuccess(_ -> startPromise.complete())
                .onFailure(startPromise::fail);
    }

    void initializeTelegramGateway() {
        synchronized (lifecycleLock) {
            clientLifecycle = ClientLifecycle.STARTING;
            if (readyPromise.future().isComplete()) readyPromise = Promise.promise();
        }
        tdlibClient = gatewayFactory.create();
        if (client == null) {
            client = new ManagedTelegramGateway(
                    this::wake,
                    () -> tdlibClient,
                    this::onRequestStarted,
                    this::onRequestFinished
            );
        }
        telegramChats = new TelegramChats(client);
        TelegramUpdateHandler telegramUpdateHandler = new TelegramUpdateHandler();
        telegramUpdateHandler.setOnAuthorizationStateUpdated(this::onAuthorizationStateUpdated);
        telegramUpdateHandler.setOnFileUpdated(this::onFileUpdated);
        telegramUpdateHandler.setOnFileDownloadsUpdated(this::onFileDownloadsUpdated);
        telegramUpdateHandler.setOnChatUpdated(telegramChats::onChatUpdated);
        telegramUpdateHandler.setOnMessageReceived(this::onMessageReceived);
        telegramUpdateHandler.setOnConnectionStateUpdated(this::onConnectionStateUpdated);

        tdlibClient.initialize(telegramUpdateHandler, this::handleException, this::handleException);
    }

    TelegramGateway tdlibClient() {
        return tdlibClient;
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        this.close(false)
                .onComplete(stopPromise);
    }

    public Future<Void> close(boolean needDelete) {
        cancelIdleSleepTimer();
        if (downloadStatusReconciliationTimerId != 0) {
            vertx.cancelTimer(downloadStatusReconciliationTimerId);
            downloadStatusReconciliationTimerId = 0;
        }
        synchronized (lifecycleLock) {
            this.needDelete = needDelete;
            permanentClose = true;
            idleClose = false;
            wakeAfterClose = false;
            if (clientLifecycle == ClientLifecycle.CLOSED) {
                return needDelete ? deleteAccountData() : Future.succeededFuture();
            }
            if (clientLifecycle == ClientLifecycle.SLEEPING) {
                clientLifecycle = ClientLifecycle.CLOSED;
                return needDelete ? deleteAccountData() : Future.succeededFuture();
            }
            if (clientLifecycle == ClientLifecycle.STOPPING && closePromise != null) return closePromise.future();
            clientLifecycle = ClientLifecycle.STOPPING;
            closePromise = Promise.promise();
        }
        tdlibClient.execute(new TdApi.Close())
                .onFailure(this::handleCloseFailure);
        return closePromise.future();
    }

    public Future<Void> wake() {
        boolean initialize = false;
        Future<Void> future;
        synchronized (lifecycleLock) {
            cancelIdleSleepTimerLocked();
            switch (clientLifecycle) {
                case ACTIVE -> { return Future.succeededFuture(); }
                case STARTING -> {
                    // A new, not-yet-registered account must be able to submit login details before
                    // AuthorizationStateReady can ever be reached.
                    return telegramRecord == null ? Future.succeededFuture() : readyPromise.future();
                }
                case STOPPING -> {
                    if (!permanentClose) wakeAfterClose = true;
                    return readyPromise.future();
                }
                case SLEEPING -> {
                    readyPromise = Promise.promise();
                    clientLifecycle = ClientLifecycle.STARTING;
                    initialize = true;
                    future = readyPromise.future();
                }
                case CLOSED -> { return Future.failedFuture("Telegram account is closed"); }
                default -> throw new IllegalStateException("Unsupported TDLib lifecycle state");
            }
        }
        if (initialize) initializeTelegramGateway();
        return future;
    }

    public void retainFrontendSession(String sessionId) {
        synchronized (lifecycleLock) {
            frontendSessions.add(sessionId);
            cancelIdleSleepTimerLocked();
        }
        wake().onFailure(e -> log.warn("[%s] Failed to wake TDLib for frontend: %s"
                .formatted(getRootId(), e.getMessage())));
    }

    public void releaseFrontendSession(String sessionId) {
        synchronized (lifecycleLock) {
            frontendSessions.remove(sessionId);
        }
        refreshIdlePolicy();
    }

    public void refreshIdlePolicy() {
        if (hasSleepBlockingAutomationEnabled()) {
            cancelIdleSleepTimer();
            wake().onFailure(e -> log.warn("[%s] Failed to keep TDLib awake for automation: %s"
                    .formatted(getRootId(), e.getMessage())));
            return;
        }
        scheduleIdleSleep();
    }

    public boolean isAvailable() {
        synchronized (lifecycleLock) {
            return authorized || (telegramRecord != null && clientLifecycle != ClientLifecycle.CLOSED);
        }
    }

    public boolean check() {
        if (StrUtil.isBlank(this.rootPath) || !FileUtil.exist(this.rootPath)) {
            log.error("[%s] Telegram account is invalid, root path: %s not exist.".formatted(this.getRootId(), this.rootPath));
            return false;
        }
        return true;
    }

    private volatile JsonObject cachedAccountJson;
    private volatile long lastAccountFetchTime = 0;

    public Future<JsonObject> getTelegramAccount() {
        return Future.future(promise -> {
            if (!authorized) {
                boolean sleeping = isSleepingOrWaking();
                JsonObject jsonObject = new JsonObject()
                        .put("id", this.getRootId())
                        .put("name", this.getRootId())
                        .put("phoneNumber", "")
                        .put("avatar", "")
                        .put("status", sleeping ? "active" : "inactive")
                        .put("sleeping", sleeping)
                        .put("rootPath", this.rootPath)
                        .put("isPremium", false)
                        .put("lastAuthorizationState", lastAuthorizationState)
                        .put("proxy", this.proxyName);
                if (this.telegramRecord != null) {
                    jsonObject.put("id", Convert.toStr(this.telegramRecord.id()))
                            .put("name", this.telegramRecord.firstName());
                }
                promise.complete(jsonObject);
                return;
            }

            // Return cached account info immediately (0ms)
            long now = System.currentTimeMillis();
            if (cachedAccountJson != null) {
                promise.complete(cachedAccountJson.copy());
                // Silently refresh in background if cache is older than 60s
                if (now - lastAccountFetchTime > 60_000L) {
                    refreshAccountCache();
                }
                return;
            }

            // No cache yet: fetch from TDLib with fallback
            fetchAccountFromTdLib(promise);
        });
    }

    private void refreshAccountCache() {
        if (!authorized) return;
        client.execute(new TdApi.GetMe())
                .onSuccess(user -> {
                    this.cachedAccountJson = buildAccountJson(user);
                    this.lastAccountFetchTime = System.currentTimeMillis();
                })
                .onFailure(e -> log.debug("[%s] Background refresh account failed: %s".formatted(this.getRootId(), e.getMessage())));
    }

    private void fetchAccountFromTdLib(Promise<JsonObject> promise) {
        client.execute(new TdApi.GetMe())
                .onSuccess(user -> {
                    JsonObject result = buildAccountJson(user);
                    this.cachedAccountJson = result;
                    this.lastAccountFetchTime = System.currentTimeMillis();
                    promise.complete(result.copy());
                })
                .onFailure(e -> {
                    log.warn("[%s] Failed to get telegram account from TDLib, fallback to local: %s"
                            .formatted(this.getRootId(), e.getMessage()));
                    // Fallback to local record to avoid HTTP 500
                    JsonObject fallback = new JsonObject()
                            .put("id", this.telegramRecord != null ? Convert.toStr(this.telegramRecord.id()) : this.getRootId())
                            .put("name", this.telegramRecord != null ? this.telegramRecord.firstName() : this.getRootId())
                            .put("phoneNumber", "")
                            .put("avatar", "")
                            .put("status", "active")
                            .put("rootPath", this.rootPath)
                            .put("isPremium", false)
                            .put("proxy", this.proxyName);
                    this.cachedAccountJson = fallback;
                    this.lastAccountFetchTime = System.currentTimeMillis();
                    promise.complete(fallback);
                });
    }

    private JsonObject buildAccountJson(TdApi.User user) {
        return new JsonObject()
                .put("id", Convert.toStr(user.id))
                .put("name", StrUtil.join(" ", user.firstName, user.lastName))
                .put("phoneNumber", user.phoneNumber)
                .put("avatar", Base64.encode((byte[]) BeanUtil.getProperty(user, "profilePhoto.minithumbnail.data")))
                .put("status", "active")
                .put("rootPath", this.rootPath)
                .put("isPremium", user.isPremium)
                .put("proxy", this.proxyName);
    }

    public Future<JsonArray> getChats(Long activatedChatId, String query, boolean archived) {
        return TelegramConverter.convertChat(this.telegramRecord.id(), telegramChats.getChatList(activatedChatId, query, 100, archived));
    }

    public TdApi.Chat getChat(long chatId) {
        return telegramChats == null ? null : telegramChats.getChat(chatId);
    }

    public boolean isForum(long chatId) {
        return telegramChats != null && telegramChats.isForum(chatId);
    }

    public Future<JsonObject> getChatFiles(long chatId, Map<String, String> filter) {
        boolean offline = Convert.toBool(filter.get("offline"), false);
        if (offline) {
            return FileRecordRetriever.getFiles(chatId, filter);
        } else {
            long messageThreadId = Convert.toLong(filter.get("messageThreadId"), 0L);
            TdApi.SearchChatMessages searchChatMessages = new TdApi.SearchChatMessages();
            searchChatMessages.chatId = chatId;
            searchChatMessages.query = filter.get("search");
            searchChatMessages.fromMessageId = Convert.toLong(filter.get("fromMessageId"), 0L);
            searchChatMessages.offset = Convert.toInt(filter.get("offset"), 0);
            searchChatMessages.limit = Convert.toInt(filter.get("limit"), 20);
            searchChatMessages.filter = TdApiHelp.getSearchMessagesFilter(filter.get("type"));
            searchChatMessages.topicId = messageThreadId > 0 ? new TdApi.MessageTopicThread(messageThreadId) : null;

            return (Objects.equals(filter.get("downloadStatus"), FileRecord.DownloadStatus.idle.name()) ?
                    this.getIdleChatFiles(searchChatMessages, 0) :
                    client.execute(searchChatMessages))
                    .compose(t -> {
                        cacheOnlineFiles(t);
                        preloadThumbnails(t)
                                .onFailure(err -> log.debug("[%s] Preload thumbnails skipped: %s"
                                        .formatted(getRootId(), err.getMessage())));
                        return TelegramConverter.convertFiles(this.telegramRecord.id(), t)
                                .compose(TelegramConverter::enrichSeedAssociations);
                    });
        }
    }

    private void cacheOnlineFiles(TdApi.FoundChatMessages foundChatMessages) {
        if (foundChatMessages == null || foundChatMessages.messages == null) {
            return;
        }
        cacheOnlineMessages(foundChatMessages.messages);
    }

    private void cacheOnlineMessages(TdApi.Message[] messages) {
        if (messages == null) {
            return;
        }
        if (onlineFilesCache.size() > 20000) {
            onlineFilesCache.clear();
        }
        for (TdApi.Message message : messages) {
            TdApiHelp.getFileHandler(message).ifPresent(handler -> {
                TdApi.File file = handler.getFile();
                if (file != null && file.remote != null && StrUtil.isNotBlank(file.remote.uniqueId)) {
                    String type = "file";
                    String mimeType = null;
                    if (handler instanceof TdApiHelp.PhotoHandler) {
                        type = "photo";
                        mimeType = "image/jpeg";
                    } else if (handler instanceof TdApiHelp.VideoHandler vh) {
                        type = "video";
                        if (vh.getContent() != null && vh.getContent().video != null) {
                            mimeType = vh.getContent().video.mimeType;
                        }
                        if (StrUtil.isBlank(mimeType)) {
                            mimeType = "video/mp4";
                        }
                    } else if (handler instanceof TdApiHelp.AudioHandler ah) {
                        type = "audio";
                        if (ah.getContent() != null && ah.getContent().audio != null) {
                            mimeType = ah.getContent().audio.mimeType;
                        }
                    }
                    long size = file.size > 0 ? file.size : file.expectedSize;
                    onlineFilesCache.put(file.remote.uniqueId, new OnlineFileInfo(file.id, type, mimeType, size));
                }
                TdApi.Thumbnail thumb = handler.getThumbnail();
                if (thumb != null && thumb.file != null && thumb.file.remote != null && StrUtil.isNotBlank(thumb.file.remote.uniqueId)) {
                    long thumbSize = thumb.file.size > 0 ? thumb.file.size : thumb.file.expectedSize;
                    onlineFilesCache.put(thumb.file.remote.uniqueId, new OnlineFileInfo(thumb.file.id, "thumbnail", TdApiHelp.getThumbnailMimeType(thumb.format), thumbSize));
                }
            });
        }
    }

    private Future<TdApi.FoundChatMessages> getIdleChatFiles(TdApi.SearchChatMessages searchChatMessages, int seq) {
        if (seq != 0) {
            // Increase the limit and reduce the number of requests
            searchChatMessages.limit = 100;
        }
        return client.execute(searchChatMessages)
                .compose(foundChatMessages -> {
                    TdApi.Message[] messages = Stream.of(foundChatMessages.messages)
                            .filter(message ->
                                    TdApiHelp.getFileHandler(message)
                                            .map(TdApiHelp.FileHandler::getFile)
                                            .map(file -> file.local == null || (
                                                    !file.local.isDownloadingActive
                                                    && !file.local.isDownloadingCompleted
                                                    && file.local.downloadedSize == 0
                                            ))
                                            .orElse(false)
                            )
                            .toArray(TdApi.Message[]::new);
                    if (ArrayUtil.isEmpty(messages) && foundChatMessages.nextFromMessageId != 0) {
                        searchChatMessages.fromMessageId = foundChatMessages.nextFromMessageId;
                        return getIdleChatFiles(searchChatMessages, seq + 1);
                    } else {
                        foundChatMessages.messages = messages;
                        return Future.succeededFuture(foundChatMessages);
                    }
                });
    }

    private final Map<Long, Tuple2<Long, JsonObject>> chatFilesCountCache = new ConcurrentHashMap<>();

    public Future<JsonObject> getChatFilesCount(long chatId) {
        Tuple2<Long, JsonObject> cached = chatFilesCountCache.get(chatId);
        if (cached != null && System.currentTimeMillis() - cached.v1 < 60_000L) {
            return Future.succeededFuture(cached.v2);
        }
        return Future.all(
                Stream.of(new TdApi.SearchMessagesFilterPhotoAndVideo(),
                                new TdApi.SearchMessagesFilterPhoto(),
                                new TdApi.SearchMessagesFilterVideo(),
                                new TdApi.SearchMessagesFilterAudio(),
                                new TdApi.SearchMessagesFilterDocument())
                        .map(filter -> client.execute(
                                                new TdApi.GetChatMessageCount(chatId,
                                                        null,
                                                        filter,
                                                        false)
                                        )
                                        .map(count -> new JsonObject()
                                                .put("type", TdApiHelp.getSearchMessagesFilterType(filter))
                                                .put("count", count.count)
                                        )
                        )
                        .toList()
        ).map(counts -> {
            JsonObject result = new JsonObject();
            counts.<JsonObject>list().forEach(count -> result.put(count.getString("type"), count.getInteger("count")));
            chatFilesCountCache.put(chatId, Tuple.tuple(System.currentTimeMillis(), result));
            return result;
        });
    }

    public Future<JsonObject> parseLink(String link) {
        return client.execute(new TdApi.GetMessageLinkInfo(link))
                .compose(messageLinkInfo -> {
                    if (messageLinkInfo.message == null) {
                        return Future.failedFuture("Message not found for link: " + link);
                    }
                    return FileRecordRetriever.getAlbumMessages(this.telegramRecord.id(), messageLinkInfo.message);
                })
                .compose(messages -> {
                    cacheOnlineMessages(messages);
                    return TelegramConverter.convertFiles(this.telegramRecord.id(), messages)
                            .map(files -> new JsonObject()
                                    .put("files", files)
                                    .put("count", files.size())
                                    .put("size", files.size())
                                    .put("nextFromMessageId", 0L) // No next message ID for link parsing
                            );
                });
    }

    public Future<Tuple2<String, String>> loadPreview(String uniqueId) {
        return DataVerticle.fileRepository
                .getByUniqueId(uniqueId)
                .compose(fileRecord -> {
                    if (fileRecord != null) {
                        if (StrUtil.isNotBlank(fileRecord.localPath()) && FileUtil.exist(fileRecord.localPath())) {
                            return Future.succeededFuture(Tuple.tuple(fileRecord.localPath(), fileRecord.mimeType()));
                        }
                        // If it is a thumbnail or photo, fetch on-demand from TDLib
                        if ("thumbnail".equals(fileRecord.type()) || "photo".equals(fileRecord.type())) {
                            return fetchMediaPreview(fileRecord.id(), fileRecord.mimeType(), fileRecord.uniqueId());
                        }
                        return Future.failedFuture("File not found or not downloaded");
                    }

                    // Fallback to online files cache for un-cached channel media
                    OnlineFileInfo onlineInfo = onlineFilesCache.get(uniqueId);
                    if (onlineInfo != null && ("thumbnail".equals(onlineInfo.type()) || "photo".equals(onlineInfo.type()))) {
                        return fetchMediaPreview(onlineInfo.fileId(), onlineInfo.mimeType(), uniqueId);
                    }

                    return Future.failedFuture("File not found");
                });
    }

    public Future<MediaTarget> resolveMediaTarget(String uniqueId) {
        return DataVerticle.fileRepository
                .getByUniqueId(uniqueId)
                .compose(fileRecord -> {
                    if (fileRecord != null) {
                        boolean exists = StrUtil.isNotBlank(fileRecord.localPath()) && FileUtil.exist(fileRecord.localPath());
                        return Future.succeededFuture(new MediaTarget(
                                fileRecord.id(),
                                fileRecord.type(),
                                fileRecord.mimeType(),
                                fileRecord.size(),
                                fileRecord.localPath(),
                                exists
                        ));
                    }

                    OnlineFileInfo onlineInfo = onlineFilesCache.get(uniqueId);
                    if (onlineInfo != null) {
                        return client.execute(new TdApi.GetFile(onlineInfo.fileId()))
                                .map(tdFile -> {
                                    boolean exists = tdFile.local != null && tdFile.local.isDownloadingCompleted
                                            && StrUtil.isNotBlank(tdFile.local.path) && FileUtil.exist(tdFile.local.path);
                                    long size = tdFile.size > 0 ? tdFile.size : (tdFile.expectedSize > 0 ? tdFile.expectedSize : onlineInfo.size());
                                    return new MediaTarget(
                                            onlineInfo.fileId(),
                                            onlineInfo.type(),
                                            onlineInfo.mimeType(),
                                            size,
                                            tdFile.local != null ? tdFile.local.path : null,
                                            exists
                                    );
                                })
                                .recover(_ -> Future.succeededFuture(new MediaTarget(
                                        onlineInfo.fileId(),
                                        onlineInfo.type(),
                                        onlineInfo.mimeType(),
                                        onlineInfo.size(),
                                        null,
                                        false
                                )));
                    }

                    return Future.failedFuture("File not found");
                });
    }

    public Future<byte[]> readMediaChunk(int fileId, long offset, long count) {
        long readCount = Math.min(count, 1024 * 1024);
        return client.execute(new TdApi.ReadFilePart(fileId, offset, readCount))
                .map(data -> data.data)
                .recover(err -> {
                    client.execute(new TdApi.DownloadFile(fileId, 32, offset, Math.max(readCount * 4, 4 * 1024 * 1024), false));

                    Promise<byte[]> promise = Promise.promise();
                    PendingChunkRead pending = new PendingChunkRead(offset, readCount, promise);
                    List<PendingChunkRead> list = pendingChunkReads.computeIfAbsent(fileId, _ -> new java.util.concurrent.CopyOnWriteArrayList<>());
                    list.add(pending);

                    long timerId = vertx.setTimer(15000, _ -> {
                        list.remove(pending);
                        promise.tryFail("Read chunk timeout: offset=" + offset);
                    });

                    return promise.future().onComplete(_ -> vertx.cancelTimer(timerId));
                });
    }

    public Future<Tuple2<String, String>> fetchMediaPreview(int fileId, String mimeType, String uniqueId) {
        return client.execute(new TdApi.DownloadFile(fileId, 32, 0, 0, false))
                .compose(file -> {
                    if (file.local != null && StrUtil.isNotBlank(file.local.path) && FileUtil.exist(file.local.path)) {
                        if (uniqueId != null) {
                            DataVerticle.fileRepository.updateDownloadStatus(file.id, uniqueId, file.local.path,
                                    FileRecord.DownloadStatus.completed, System.currentTimeMillis());
                        }
                        return Future.succeededFuture(Tuple.tuple(file.local.path, mimeType));
                    }
                    Promise<String> promise = Promise.promise();
                    List<Promise<String>> list = pendingPreviewPromises.computeIfAbsent(file.id, _ -> new java.util.concurrent.CopyOnWriteArrayList<>());
                    list.add(promise);
                    long timerId = vertx.setTimer(15000, _ -> {
                        list.remove(promise);
                        promise.tryFail("Preview download timeout");
                    });
                    return promise.future()
                            .onComplete(_ -> vertx.cancelTimer(timerId))
                            .map(path -> {
                                if (uniqueId != null) {
                                    DataVerticle.fileRepository.updateDownloadStatus(fileId, uniqueId, path,
                                            FileRecord.DownloadStatus.completed, System.currentTimeMillis());
                                }
                                return Tuple.tuple(path, mimeType);
                            });
                });
    }

    public Future<FileRecord> startDownload(Long chatId, Long messageId, Integer fileId) {
        if (unifiedFileDownloadService == null) {
            return startTelegramDownload(chatId, messageId, fileId);
        }
        return client.execute(new TdApi.GetFile(fileId)).compose(file ->
                unifiedFileDownloadService.downloadIfAvailable(file.remote.uniqueId, file.size)
                        .recover(failure -> {
                            String reason = failure instanceof HttpSeedCoordinatorClient.SeedProtocolException protocol
                                    ? "platform status=" + protocol.statusCode() + " code=" + protocol.errorCode()
                                    : failure.getClass().getSimpleName();
                            log.debug("[{}] Seed download unavailable for {}: {}",
                                    getRootId(), file.remote.uniqueId, reason);
                            return Future.succeededFuture(false);
                        })
                        .compose(handled -> handled
                                ? ensureFileRecord(chatId, messageId, file)
                                : startTelegramDownload(chatId, messageId, fileId))
        );
    }

    private Future<FileRecord> ensureFileRecord(Long chatId, Long messageId, TdApi.File file) {
        return DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId).compose(existing -> {
            if (existing != null) return Future.succeededFuture(existing);
            return Future.all(
                    client.execute(new TdApi.GetMessage(chatId, messageId)),
                    client.execute(new TdApi.GetMessageThread(chatId, messageId), true)
            ).compose(results -> {
                TdApi.Message message = results.resultAt(0);
                TdApi.MessageThreadInfo thread = results.resultAt(1);
                FileRecord record = TdApiHelp.getFileHandler(message)
                        .orElseThrow(() -> VertxException.noStackTrace("not support message type"))
                        .convertFileRecord(telegramRecord.id()).withThreadInfo(thread);
                return DataVerticle.fileRepository.createIfNotExist(record).map(record);
            });
        });
    }

    private Future<FileRecord> startTelegramDownload(Long chatId, Long messageId, Integer fileId) {
        return Future.all(
                        client.execute(new TdApi.GetFile(fileId)),
                        client.execute(new TdApi.GetMessage(chatId, messageId)),
                        client.execute(new TdApi.GetMessageThread(chatId, messageId), true)
                )
                .compose(results -> {
                    TdApi.File file = results.resultAt(0);
                    return DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                            .map(fileRecord -> Tuple.tuple(file,
                                    results.<TdApi.Message>resultAt(1),
                                    results.<TdApi.MessageThreadInfo>resultAt(2),
                                    fileRecord
                            ));
                })
                .compose(results -> {
                    TdApi.File file = results.v1;
                    TdApi.Message message = results.v2;
                    TdApi.MessageThreadInfo messageThreadInfo = results.v3;
                    FileRecord dbFileRecord = results.v4;
                    if (file.local != null) {
                        if (file.local.isDownloadingCompleted) {
                            return syncFileDownloadStatus(file, message, messageThreadInfo)
                                    .compose(_ -> DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId));
                        }
                        if (file.local.isDownloadingActive) {
                            return Future.failedFuture("File is downloading");
                        }
//                        return Future.failedFuture("Unknown file download status");
                    }
                    if (dbFileRecord != null && !dbFileRecord.isDownloadStatus(FileRecord.DownloadStatus.idle)) {
                        return Future.failedFuture("File is already downloading or completed");
                    }

                    TdApiHelp.FileHandler<? extends TdApi.MessageContent> fileHandler = TdApiHelp.getFileHandler(message)
                            .orElseThrow(() -> VertxException.noStackTrace("not support message type"));
                    FileRecord fileRecord = fileHandler.convertFileRecord(telegramRecord.id()).withThreadInfo(messageThreadInfo);
                    return DataVerticle.fileRepository.createIfNotExist(fileRecord)
                            .compose(created -> {
                                if (!created) {
                                    return DataVerticle.fileRepository.updateFileId(fileRecord.id(), fileRecord.uniqueId());
                                }
                                return Future.succeededFuture();
                            })
                            .compose(ignore -> {
                                long downloadStartDate = System.currentTimeMillis();
                                return DataVerticle.fileRepository
                                        .claimDownloadStart(fileId, fileRecord.uniqueId(), downloadStartDate)
                                        .compose(claimed -> {
                                            if (!claimed) {
                                                return Future.failedFuture("File was claimed by another download");
                                            }
                                            return client.execute(new TdApi.AddFileToDownloads(fileId, chatId, messageId, 32))
                                                    .recover(error -> DataVerticle.fileRepository
                                                            .releaseStaleDownloadRetry(fileRecord.uniqueId(), downloadStartDate)
                                                            .onFailure(releaseError -> log.error(
                                                                    "[{}] Failed to release download claim {}: {}",
                                                                    getRootId(), fileRecord.uniqueId(), releaseError.getMessage()))
                                                            .compose(_ -> Future.failedFuture(error)));
                                        });
                            })
                            .onSuccess(ignore -> {
                                sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                                        .put("fileId", fileId)
                                        .put("uniqueId", fileRecord.uniqueId())
                                        .put("downloadStatus", FileRecord.DownloadStatus.downloading)
                                ));

                                downloadThumbnail(chatId, messageId, fileHandler.convertThumbnailRecord(telegramRecord.id()));
                            })
                            .map(fileRecord);
                });
    }

    public Future<Boolean> downloadThumbnail(Long chatId, Long messageId, FileRecord thumbnailRecord) {
        if (thumbnailRecord == null) {
            return Future.succeededFuture(false);
        }
        return DataVerticle.fileRepository.createIfNotExist(thumbnailRecord)
                .compose(created -> {
                    if (!created) {
                        return DataVerticle.fileRepository.updateFileId(thumbnailRecord.id(), thumbnailRecord.uniqueId());
                    }
                    return Future.succeededFuture();
                })
                .compose(ignore -> {
                    if (thumbnailRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)) {
                        return Future.succeededFuture(false);
                    }
                    return client.execute(new TdApi.AddFileToDownloads(thumbnailRecord.id(), chatId, messageId, 32))
                            .map(true);
                })
                .onSuccess(download -> {
                    if (download) {
                        log.debug("[%s] Download thumbnail: %s".formatted(this.getRootId(), thumbnailRecord.uniqueId()));
                    }
                });
    }

    /**
     * Eagerly downloads the lightweight thumbnails for the given messages so previews are crisp
     * while browsing, without having to download the full media. Fire-and-forget: already
     * downloaded thumbnails are skipped by {@link #downloadThumbnail}.
     */
    private static final int MAX_PRELOAD_THUMBNAILS = 30;

    private static final int PRELOAD_THUMBNAIL_CONCURRENCY = 3;

    Future<Void> preloadThumbnails(TdApi.FoundChatMessages foundChatMessages) {
        if (foundChatMessages == null || foundChatMessages.messages == null || telegramRecord == null) {
            return Future.succeededFuture();
        }
        return DataVerticle.settingRepository.<Boolean>getByKey(SettingKey.thumbnailAutoLoad)
                .compose(autoLoad -> {
                    if (!Boolean.TRUE.equals(autoLoad)) {
                        return Future.succeededFuture();
                    }
                    startPreloadThumbnails(foundChatMessages);
                    return Future.succeededFuture();
                });
    }

    private void startPreloadThumbnails(TdApi.FoundChatMessages foundChatMessages) {
        // Collect at most MAX_PRELOAD_THUMBNAILS thumbnail records for this page, then download them
        // with bounded concurrency, so opening large pages or fast scrolling can't burst TDLib, the
        // DB and websocket with one download per message.
        List<FileRecord> thumbnails = new ArrayList<>();
        for (TdApi.Message message : foundChatMessages.messages) {
            if (thumbnails.size() >= MAX_PRELOAD_THUMBNAILS) {
                break;
            }
            FileRecord thumbnailRecord = TdApiHelp.getFileHandler(message)
                    .map(fileHandler -> fileHandler.convertThumbnailRecord(telegramRecord.id()))
                    .orElse(null);
            if (thumbnailRecord != null) {
                thumbnails.add(thumbnailRecord);
            }
        }
        if (thumbnails.isEmpty()) {
            return;
        }
        AtomicInteger index = new AtomicInteger(0);
        for (int i = 0; i < Math.min(PRELOAD_THUMBNAIL_CONCURRENCY, thumbnails.size()); i++) {
            preloadNextThumbnail(thumbnails, index);
        }
    }

    private void preloadNextThumbnail(List<FileRecord> thumbnails, AtomicInteger index) {
        int i = index.getAndIncrement();
        if (i >= thumbnails.size()) {
            return;
        }
        FileRecord thumbnailRecord = thumbnails.get(i);
        downloadThumbnail(thumbnailRecord.chatId(), thumbnailRecord.messageId(), thumbnailRecord)
                .onFailure(err -> log.debug("[%s] Preload thumbnail failed for %s: %s"
                        .formatted(getRootId(), thumbnailRecord.uniqueId(), err.getMessage())))
                .onComplete(_ -> preloadNextThumbnail(thumbnails, index));
    }

    public Future<Void> cancelDownload(Integer fileId) {
        if (unifiedFileDownloadService == null) return cancelTelegramDownload(fileId);
        return client.execute(new TdApi.GetFile(fileId)).compose(file ->
                unifiedFileDownloadService.controlIfPresent(file.remote.uniqueId, "CANCEL_V1")
                        .compose(handled -> handled
                                ? Future.succeededFuture()
                                : cancelTelegramDownload(fileId))
        );
    }

    private Future<Void> cancelTelegramDownload(Integer fileId) {
        return client.execute(new TdApi.GetFile(fileId))
                .compose(file -> DataVerticle.fileRepository
                        .updateFileId(file.id, file.remote.uniqueId)
                        .map(file)
                )
                .compose(file -> {
                    if (file.local == null) {
                        return Future.failedFuture("File not started downloading");
                    }

                    return client.execute(new TdApi.CancelDownloadFile(fileId, false))
                            .map(file);
                })
                .compose(file -> client.execute(new TdApi.DeleteFile(fileId)).map(file))
                .compose(file -> DataVerticle.fileRepository.deleteByUniqueId(file.remote.uniqueId).map(file))
                .onSuccess(file ->
                        sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                                .put("fileId", fileId)
                                .put("uniqueId", file.remote.uniqueId)
                                .put("downloadStatus", FileRecord.DownloadStatus.idle)
                        )))
                .mapEmpty();
    }

    public Future<Void> togglePauseDownload(Integer fileId, boolean isPaused) {
        if (unifiedFileDownloadService == null) return toggleTelegramDownload(fileId, isPaused);
        String controlType = isPaused ? "PAUSE_V1" : "RESUME_V1";
        return client.execute(new TdApi.GetFile(fileId)).compose(file ->
                unifiedFileDownloadService.controlIfPresent(file.remote.uniqueId, controlType)
                        .compose(handled -> handled
                                ? Future.succeededFuture()
                                : toggleTelegramDownload(fileId, isPaused))
        );
    }

    private Future<Void> toggleTelegramDownload(Integer fileId, boolean isPaused) {
        return client.execute(new TdApi.GetFile(fileId))
                .compose(file -> DataVerticle.fileRepository
                        .updateFileId(file.id, file.remote.uniqueId)
                        .map(file)
                )
                .compose(file -> {
                    if (file.local == null) {
                        return Future.failedFuture("File not started downloading");
                    }
                    if (file.local.isDownloadingCompleted) {
                        return syncFileDownloadStatus(file, null, null).mapEmpty();
                    }
                    if (isPaused && !file.local.isDownloadingActive) {
                        return Future.failedFuture("File is not downloading");
                    }
                    if (!isPaused && file.local.isDownloadingActive) {
                        return Future.failedFuture("File is downloading");
                    }
                    if (!isPaused && !file.local.canBeDeleted) {
                        // Maybe the file is not exist, so we need to redownload it
                        return DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                                .compose(fileRecord ->
                                        client.execute(new TdApi.AddFileToDownloads(fileId, fileRecord.chatId(), fileRecord.messageId(), 32)))
                                .mapEmpty();
                    }

                    return client.execute(new TdApi.ToggleDownloadIsPaused(fileId, isPaused));
                })
                .mapEmpty();
    }

    public Future<Void> removeFile(Integer fileId, String uniqueId) {
        return client.execute(new TdApi.GetFile(fileId))
                .otherwise((TdApi.File) null)
                .compose(file -> DataVerticle.fileRepository
                        .getByUniqueId(uniqueId)
                        .map(fileRecord -> Tuple.tuple(file, fileRecord))
                )
                .compose(tuple2 -> {
                    TdApi.File file = tuple2.v1;
                    FileRecord fileRecord = tuple2.v2;
                    if (fileRecord == null) {
                        return Future.failedFuture("File not found");
                    }

                    if (fileRecord.isTransferStatus(FileRecord.TransferStatus.completed)) {
                        if (FileUtil.del(fileRecord.localPath())) {
                            log.debug("[%s] Remove file success: %s".formatted(this.getRootId(), fileRecord.localPath()));
                        }
                    }

                    if (file != null && file.local != null && StrUtil.isNotBlank(file.local.path)) {
                        return client.execute(new TdApi.DeleteFile(fileId))
                                .map(file);
                    } else if (!fileRecord.isTransferStatus(FileRecord.TransferStatus.completed)
                               && StrUtil.isNotBlank(fileRecord.localPath())) {
                        if (FileUtil.del(fileRecord.localPath())) {
                            log.debug("[%s] Remove file success: %s".formatted(this.getRootId(), fileRecord.localPath()));
                        }
                    }
                    return Future.succeededFuture(file);
                })
                .compose(file -> DataVerticle.fileRepository.deleteByUniqueId(uniqueId).map(file))
                .onSuccess(_ -> sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                        .put("fileId", fileId)
                        .put("uniqueId", uniqueId)
                        .put("removed", true)
                )))
                .mapEmpty();
    }

    public Future<Void> updateAutoSettings(Long chatId, JsonObject params) {
        return DataVerticle.settingRepository.<SettingAutoRecords>getByKey(SettingKey.automation)
                .compose(settingAutoRecords -> {
                    if (settingAutoRecords == null) {
                        settingAutoRecords = new SettingAutoRecords();
                    }
                    SettingAutoRecords.Automation automation = params.mapTo(SettingAutoRecords.Automation.class);
                    normalizeAutomation(automation);
                    validateArchiveRule(settingAutoRecords, chatId, automation.archive);
                    boolean hasEnabled = automation.preload.enabled
                                         || automation.download.enabled
                                         || automation.transfer.enabled
                                         || automation.archive.enabled;
                    boolean hasConfiguredArchive = automation.archive.rule.targetChatId != 0;
                    boolean hasConfiguredTransfer = StrUtil.isNotBlank(automation.transfer.rule.destination);

                    if (settingAutoRecords.exists(this.telegramRecord.id(), chatId)
                        && !hasEnabled && !hasConfiguredArchive && !hasConfiguredTransfer) {
                        settingAutoRecords.remove(this.telegramRecord.id(), chatId);
                    } else {
                        if (!hasEnabled && !hasConfiguredArchive && !hasConfiguredTransfer) {
                            return Future.succeededFuture();
                        }
                        automation.telegramId = this.telegramRecord.id();
                        automation.chatId = chatId;
                        settingAutoRecords.add(automation);
                    }

                    return DataVerticle.settingRepository.createOrUpdate(SettingKey.automation.name(), Json.encode(settingAutoRecords))
                            .onSuccess(r -> vertx.eventBus().publish(EventEnum.AUTO_DOWNLOAD_UPDATE.name(), r.value()));
                })
                .mapEmpty();
    }

    private static void normalizeAutomation(SettingAutoRecords.Automation automation) {
        if (automation.preload == null) automation.preload = new SettingAutoRecords.PreloadConfig();
        if (automation.download == null) automation.download = new SettingAutoRecords.DownloadConfig();
        if (automation.download.rule == null) automation.download.rule = new SettingAutoRecords.DownloadRule();
        if (automation.transfer == null) automation.transfer = new SettingAutoRecords.TransferConfig();
        if (automation.transfer.rule == null) automation.transfer.rule = new SettingAutoRecords.TransferRule();
        if (automation.archive == null) automation.archive = new SettingAutoRecords.ArchiveConfig();
        if (automation.archive.rule == null) automation.archive.rule = new SettingAutoRecords.ArchiveRule();
        if (automation.archive.rule.mode == null) {
            automation.archive.rule.mode = SettingAutoRecords.ArchiveMode.COPY;
        }
        if (automation.archive.rule.initialSyncMode == null) {
            automation.archive.rule.initialSyncMode = SettingAutoRecords.ArchiveInitialSyncMode.NOW;
        }
        if (automation.archive.rule.strictOrder) {
            automation.archive.rule.recoveryEnabled = true;
        }
        if (automation.archive.rule.topicMode == null) {
            automation.archive.rule.topicMode = SettingAutoRecords.ArchiveTopicMode.MERGE;
        }
        if (automation.archive.rule.scope == null) {
            automation.archive.rule.scope = SettingAutoRecords.ArchiveScope.ALL_MESSAGES;
        }
        if (automation.archive.rule.fileTypes == null) {
            automation.archive.rule.fileTypes = new ArrayList<>();
        }
    }

    private void validateArchiveRule(SettingAutoRecords existing,
                                     long sourceChatId,
                                     SettingAutoRecords.ArchiveConfig archive) {
        if (archive == null || archive.rule == null || archive.rule.targetChatId == 0) {
            return;
        }
        long targetChatId = archive.rule.targetChatId;
        SettingAutoRecords.ArchiveTopicMode topicMode = archive.rule.topicMode == null
                ? SettingAutoRecords.ArchiveTopicMode.MERGE : archive.rule.topicMode;
        if (topicMode == SettingAutoRecords.ArchiveTopicMode.PRESERVE
            && (!isForum(sourceChatId) || !isForum(targetChatId))) {
            throw new IllegalArgumentException(
                    "Preserving topics requires both source and destination to be forum groups");
        }
        if (sourceChatId == targetChatId) {
            if (archive.rule.sourceTopicId == 0 || archive.rule.targetTopicId == 0
                || archive.rule.sourceTopicId == archive.rule.targetTopicId) {
                throw new IllegalArgumentException(
                        "Source and destination must be different chats or different forum topics");
            }
            return;
        }
        Set<Long> visited = new HashSet<>();
        long cursor = targetChatId;
        while (visited.add(cursor)) {
            if (cursor == sourceChatId) {
                throw new IllegalArgumentException("Cloud archive rules can't form a forwarding loop");
            }
            SettingAutoRecords.Automation next = existing.getItem(this.telegramRecord.id(), cursor);
            if (next == null || next.chatId == sourceChatId || next.archive == null
                || !next.archive.enabled || next.archive.rule == null
                || next.archive.rule.targetChatId == 0) {
                break;
            }
            cursor = next.archive.rule.targetChatId;
        }
    }

    public Future<JsonObject> getDownloadStatistics() {
        return Future.all(DataVerticle.fileRepository.getDownloadStatistics(this.telegramRecord.id()),
                client.execute(new TdApi.GetNetworkStatistics())
        ).map(r -> {
            JsonObject jsonObject = r.resultAt(0);
            TdApi.NetworkStatistics networkStatistics = r.resultAt(1);
            Tuple2<Long, Long> bytes = Arrays.stream(networkStatistics.entries)
                    .filter(e -> e instanceof TdApi.NetworkStatisticsEntryFile)
                    .map(e -> {
                        TdApi.NetworkStatisticsEntryFile entry = (TdApi.NetworkStatisticsEntryFile) e;
                        return Tuple.tuple(entry.sentBytes, entry.receivedBytes);
                    })
                    .reduce((a, b) -> Tuple.tuple(a.v1 + b.v1, a.v2 + b.v2))
                    .orElse(Tuple.tuple(0L, 0L));

            jsonObject.put("networkStatistics", JsonObject.of()
                    .put("sinceDate", networkStatistics.sinceDate)
                    .put("sentBytes", bytes.v1)
                    .put("receivedBytes", bytes.v2)
            );

            jsonObject.put("speedStats", avgSpeed.getSpeedStats());
            return jsonObject;
        });
    }

    public Future<JsonObject> getDownloadStatisticsByPhase(Integer timeRange) {
        // 1: 1 hour, 2: 1 day, 3: 1 week, 4: 1 month
        long endTime = System.currentTimeMillis();
        long startTime = switch (timeRange) {
            case 1 -> DateUtil.offsetHour(DateUtil.date(), -1).getTime();
            case 2 -> DateUtil.offsetDay(DateUtil.date(), -1).getTime();
            case 3 -> DateUtil.offsetWeek(DateUtil.date(), -1).getTime();
            case 4 -> DateUtil.offsetMonth(DateUtil.date(), -1).getTime();
            default -> throw new IllegalStateException("Unexpected value: " + timeRange);
        };

        return Future.all(
                        DataVerticle.statisticRepository.getRangeStatistics(StatisticRecord.Type.speed, this.telegramRecord.id(), startTime, endTime)
                                .map(statisticRecords -> TelegramConverter.convertRangedSpeedStats(statisticRecords, timeRange)),
                        DataVerticle.fileRepository.getCompletedRangeStatistics(this.telegramRecord.id(), startTime, endTime, timeRange)
                )
                .map(r -> new JsonObject()
                        .put("speedStats", r.resultAt(0))
                        .put("completedStats", r.resultAt(1))
                );
    }

    public Future<TdApi.AddedProxy> enableProxy(String proxyName) {
        return enableProxy(proxyName, client);
    }

    private Future<TdApi.AddedProxy> enableProxy(String proxyName, TelegramGateway gateway) {
        if (StrUtil.isBlank(proxyName)) return Future.succeededFuture();
        return DataVerticle.settingRepository.<SettingProxyRecords>getByKey(SettingKey.proxys)
                .map(settingProxyRecords -> Optional.ofNullable(settingProxyRecords)
                        .flatMap(r -> r.getProxy(proxyName))
                        .orElseThrow(() -> VertxException.noStackTrace("Proxy %s not found".formatted(proxyName)))
                )
                .compose(proxy -> this.getTdAddedProxy(proxy, gateway)
                        .map(r -> Tuple.tuple(proxy, r))
                )
                .compose(tuple -> {
                    SettingProxyRecords.Item proxy = tuple.v1;
                    TdApi.AddedProxy tdAddedProxy = tuple.v2;
                    boolean edit = false;
                    if (tdAddedProxy != null) {
                        if (tdAddedProxy.isEnabled) {
                            return Future.succeededFuture(tdAddedProxy);
                        }
                        edit = true;
                    }

                    TdApi.ProxyType proxyType;
                    switch (proxy.type) {
                        case "http" -> proxyType = new TdApi.ProxyTypeHttp(proxy.username, proxy.password, false);
                        case "socks5" -> proxyType = new TdApi.ProxyTypeSocks5(proxy.username, proxy.password);
                        case "mtproto" -> proxyType = new TdApi.ProxyTypeMtproto(proxy.secret);
                        case null, default -> {
                            return Future.failedFuture("Unsupported proxy type: %s".formatted(proxy.type));
                        }
                    }
                    TdApi.Proxy tdProxy = new TdApi.Proxy(proxy.server, proxy.port, proxyType);
                    return edit ? gateway.execute(new TdApi.EditProxy(tdAddedProxy.id, tdProxy, true, proxy.name))
                            : gateway.execute(new TdApi.AddProxy(tdProxy, true, proxy.name));
                })
                .compose( r -> {
                    this.proxyName = proxyName;
                    if (this.telegramRecord != null) {
                        return DataVerticle.telegramRepository.update(this.telegramRecord.withProxy(proxyName))
                                .onSuccess(telegramRecord -> this.telegramRecord = telegramRecord)
                                .map(r);
                    } else {
                        return Future.succeededFuture(r);
                    }
                });
    }

    public Future<TdApi.AddedProxy> toggleProxy(JsonObject jsonObject) {
        String toggleProxyName = jsonObject.getString("proxyName");
        if (Objects.equals(toggleProxyName, this.proxyName)) {
            return Future.succeededFuture();
        }

        if (StrUtil.isBlank(toggleProxyName) && StrUtil.isNotBlank(this.proxyName)) {
            // disable proxy
            return client.execute(new TdApi.DisableProxy())
                    .compose(_ -> {
                        this.proxyName = null;
                        if (this.telegramRecord != null) {
                            return DataVerticle.telegramRepository.update(this.telegramRecord.withProxy(null))
                                    .onSuccess(telegramRecord -> this.telegramRecord = telegramRecord)
                                    .mapEmpty();
                        }
                        return Future.succeededFuture();
                    });
        } else {
            return this.enableProxy(toggleProxyName);
        }
    }

    public Future<TdApi.AddedProxy> getTdAddedProxy(SettingProxyRecords.Item proxy) {
        return getTdAddedProxy(proxy, client);
    }

    private Future<TdApi.AddedProxy> getTdAddedProxy(SettingProxyRecords.Item proxy, TelegramGateway gateway) {
        return gateway.execute(new TdApi.GetProxies())
                .map(proxies -> Stream.of(proxies.proxies)
                        .filter(p -> proxy.equalsTdProxy(p.proxy))
                        .findFirst()
                        .orElse(null));
    }

    public Future<TdApi.Proxy> getEnabledTdProxy() {
        return client.execute(new TdApi.GetProxies())
                .map(proxies -> Stream.of(proxies.proxies)
                        .filter(p -> p.isEnabled)
                        .map(p -> p.proxy)
                        .findFirst()
                        .orElse(null));
    }

    public Future<Double> ping() {
        return this.getEnabledTdProxy()
                .compose(proxy -> client.execute(new TdApi.PingProxy(proxy)))
                .map(r -> r.seconds);
    }

    public Future<String> execute(String method, Object params) {
        String code = RandomUtil.randomString(10);
        log.trace("[%s] Execute code: %s method: %s, params: %s".formatted(getRootId(), code, method, params));
        return Future.future(promise -> {
            TdApi.Function<?> func = TdApiHelp.getFunction(method, params);
            if (func == null) {
                promise.fail("Unsupported method: " + method);
                return;
            }
            client.send(func, object -> {
                log.debug("[%s] Execute: [%s] Receive result: %s".formatted(getRootId(), code, object));
                handleDefaultResult(object, code);
            });
            promise.complete(code);
        });
    }

    private void sendEvent(EventPayload payload) {
        vertx.eventBus().publish(EventEnum.TELEGRAM_EVENT.address(),
                JsonObject.of("telegramId", this.getId(), "payload", JsonObject.mapFrom(payload)));
    }

    private void sendFileStatusHttpEvent(TdApi.File file, JsonObject fileUpdated) {
        if (fileUpdated == null || fileUpdated.isEmpty()) return;

        JsonObject statusData = new JsonObject()
                .put("fileId", file.id)
                .put("uniqueId", file.remote.uniqueId)
                .put("downloadStatus", fileUpdated.getString("downloadStatus"))
                .put("localPath", fileUpdated.getString("localPath"))
                .put("completionDate", fileUpdated.getLong("completionDate"))
                .put("downloadedSize", file.local.downloadedSize);

        // 如果文件下载完成，尝试获取并包含缩略图文件信息
        if ("completed".equals(fileUpdated.getString("downloadStatus"))) {
            DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                    .compose(mainFileRecord -> {
                        if (mainFileRecord != null) {
                            statusData.put("type", mainFileRecord.type());
                        }
                        if (mainFileRecord != null && mainFileRecord.thumbnailUniqueId() != null) {
                            return FileRecordRetriever.getThumbnails(List.of(mainFileRecord))
                                    .map(thumbnailMap -> {
                                        FileRecord thumbnailRecord = thumbnailMap.get(mainFileRecord.thumbnailUniqueId());
                                        if (thumbnailRecord != null && thumbnailRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)) {
                                            statusData.put("thumbnailFile", JsonObject.of(
                                                    "uniqueId", thumbnailRecord.uniqueId(),
                                                    "mimeType", thumbnailRecord.mimeType(),
                                                    "extra", StrUtil.isBlank(thumbnailRecord.extra()) ? null : Json.decodeValue(thumbnailRecord.extra())
                                            ));
                                        }
                                        return statusData;
                                    });
                        }
                        return Future.succeededFuture(statusData);
                    })
                    .onSuccess(finalStatusData -> sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, finalStatusData)))
                    .onFailure(err -> {
                        // 如果获取缩略图失败，仍然发送基本状态信息
                        log.error("Failed to get thumbnail info for file: %s, error: %s".formatted(file.remote.uniqueId, err.getMessage()));
                        sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, statusData));
                    });
        } else {
            // 非完成状态直接发送
            sendEvent(EventPayload.build(EventPayload.TYPE_FILE_STATUS, statusData));
        }
    }

    private void handleAuthorizationResult(TdApi.Object object) {
        switch (object.getConstructor()) {
            case TdApi.Error.CONSTRUCTOR:
                sendEvent(EventPayload.build(EventPayload.TYPE_ERROR, object));
                break;
            case TdApi.Ok.CONSTRUCTOR:
                break;
            default:
                log.warn("[%s] Receive UpdateAuthorizationState with invalid authorization state%s".formatted(getRootId(), object));
        }
    }

    private void handleDefaultResult(TdApi.Object object, String code) {
        if (object.getConstructor() == TdApi.Error.CONSTRUCTOR) {
            sendEvent(EventPayload.build(EventPayload.TYPE_ERROR, code, object));
        } else {
            sendEvent(EventPayload.build(EventPayload.TYPE_METHOD_RESULT, code, object));
        }
    }

    private void handleException(Throwable e) {
        log.error(e);
    }

    private Future<Void> cleanupOldVerticle(String oldRootPath) {
        if (oldRootPath.equals(this.rootPath)) {
            return Future.succeededFuture();
        }
        Optional<TelegramVerticle> found = TelegramVerticles.getAll().stream()
                .filter(v -> v != this && oldRootPath.equals(v.rootPath))
                .findFirst();
        if (found.isEmpty()) {
            return Future.succeededFuture();
        }
        TelegramVerticle old = found.get();
        log.info("[%s] Replacing stale verticle at path: %s".formatted(getRootId(), oldRootPath));
        String deployId = old.deploymentID();
        if (deployId == null) {
            // Not deployed; just drop it from the registry.
            TelegramVerticles.remove(old);
            return Future.succeededFuture();
        }
        // Undeploy first; only drop it from the registry once undeploy succeeds. A failed undeploy
        // must not leave a still-running verticle that is no longer tracked/manageable.
        return vertx.undeploy(deployId)
                .onSuccess(_ -> TelegramVerticles.remove(old))
                .recover(e -> {
                    log.warn("[%s] Could not undeploy stale verticle, keeping it registered: %s"
                            .formatted(getRootId(), e.getMessage()));
                    return Future.succeededFuture();
                })
                .mapEmpty();
    }

    private void handleSaveAvgSpeed() {
        if (!authorized || telegramRecord == null) return;
        AvgSpeed.SpeedStats speedStats = avgSpeed.getSpeedStats();
        if (speedStats.avgSpeed() == 0
            && speedStats.minSpeed() == 0
            && speedStats.medianSpeed() == 0
            && speedStats.maxSpeed() == 0) {
            return;
        }
        JsonObject data = JsonObject.mapFrom(speedStats);
        data.remove("interval");
        DataVerticle.statisticRepository.create(new StatisticRecord(Convert.toStr(telegramRecord.id()),
                StatisticRecord.Type.speed,
                System.currentTimeMillis(),
                data.encode()));

        // Avoid speed not being updated for a long time
        avgSpeed.update(0, System.currentTimeMillis());
    }

    private Future<Void> initAvgSpeed() {
        return DataVerticle.settingRepository.<Integer>getByKey(SettingKey.avgSpeedInterval)
                .compose(interval -> {
                    if (Objects.equals(interval, avgSpeed.getSpeedStats().interval())) {
                        if (avgSpeedPersistenceTimerId == 0) {
                            avgSpeedPersistenceTimerId = vertx.setPeriodic(interval * 1000, _ -> handleSaveAvgSpeed());
                        }
                        return Future.succeededFuture();
                    }

                    avgSpeed = new AvgSpeed(interval);
                    if (avgSpeedPersistenceTimerId != 0) {
                        vertx.cancelTimer(avgSpeedPersistenceTimerId);
                    }
                    avgSpeedPersistenceTimerId = vertx.setPeriodic(interval * 1000, _ -> handleSaveAvgSpeed());
                    return Future.succeededFuture();
                });
    }

    private Future<Void> initEventConsumer() {
        vertx.eventBus().consumer(EventEnum.SETTING_UPDATE.address(SettingKey.avgSpeedInterval.name()), message -> {
            log.debug("Avg Speed Interval update: %s".formatted(message.body()));
            this.initAvgSpeed();
        });
        vertx.eventBus().consumer(EventEnum.SETTING_UPDATE.address(SettingKey.tdlibIdleTimeoutMinutes.name()), message -> {
            idleTimeoutMinutes = Math.max(0, Convert.toInt(message.body(), 0));
            refreshIdlePolicy();
        });

        return Future.succeededFuture();
    }

    private Future<Void> initIdleSleep() {
        return DataVerticle.settingRepository.<Integer>getByKey(SettingKey.tdlibIdleTimeoutMinutes)
                .onSuccess(value -> {
                    idleTimeoutMinutes = Math.max(0, value == null ? 0 : value);
                    refreshIdlePolicy();
                })
                .mapEmpty();
    }

    private void onRequestStarted() {
        synchronized (lifecycleLock) {
            activeRequests++;
            cancelIdleSleepTimerLocked();
        }
    }

    private void onRequestFinished() {
        synchronized (lifecycleLock) {
            if (activeRequests > 0) activeRequests--;
        }
        scheduleIdleSleep();
    }

    private boolean hasSleepBlockingAutomationEnabled() {
        if (telegramRecord == null) return false;
        return AutomationsHolder.INSTANCE.autoRecords().automations.stream()
                .anyMatch(auto -> auto.telegramId == telegramRecord.id()
                                  && ((auto.download != null && auto.download.enabled)
                                      || (auto.preload != null && auto.preload.enabled)
                                      || (auto.archive != null && auto.archive.enabled)));
    }

    private boolean canIdleSleepLocked() {
        return idleTimeoutMinutes > 0
               && clientLifecycle == ClientLifecycle.ACTIVE
               && frontendSessions.isEmpty()
               && activeRequests == 0
               && !downloadsActive
               && !hasSleepBlockingAutomationEnabled();
    }

    private void scheduleIdleSleep() {
        synchronized (lifecycleLock) {
            cancelIdleSleepTimerLocked();
            if (!canIdleSleepLocked() || vertx == null) return;
            idleSleepTimerId = vertx.setTimer(idleTimeoutMinutes * 60_000L, _ -> closeForIdle());
        }
    }

    private void closeForIdle() {
        synchronized (lifecycleLock) {
            idleSleepTimerId = 0;
            if (!canIdleSleepLocked()) return;
            idleClose = true;
            permanentClose = false;
            wakeAfterClose = false;
            clientLifecycle = ClientLifecycle.STOPPING;
            closePromise = Promise.promise();
            readyPromise = Promise.promise();
        }
        log.info("[%s] Closing idle TDLib client".formatted(getRootId()));
        tdlibClient.execute(new TdApi.Close()).onFailure(this::handleCloseFailure);
    }

    private void handleCloseFailure(Throwable failure) {
        Promise<Void> pendingClose;
        synchronized (lifecycleLock) {
            clientLifecycle = ClientLifecycle.ACTIVE;
            idleClose = false;
            permanentClose = false;
            pendingClose = closePromise;
            closePromise = null;
            if (!readyPromise.future().isComplete()) readyPromise.complete();
        }
        if (pendingClose != null && !pendingClose.future().isComplete()) pendingClose.fail(failure);
        log.error("[%s] Failed to close Telegram account: %s".formatted(getRootId(), failure.getMessage()));
        scheduleIdleSleep();
    }

    private boolean isSleepingOrWaking() {
        synchronized (lifecycleLock) {
            return telegramRecord != null
                   && (clientLifecycle == ClientLifecycle.SLEEPING
                       || (clientLifecycle == ClientLifecycle.STARTING && !permanentClose)
                       || (clientLifecycle == ClientLifecycle.STOPPING && idleClose));
        }
    }

    private void cancelIdleSleepTimer() {
        synchronized (lifecycleLock) {
            cancelIdleSleepTimerLocked();
        }
    }

    private void cancelIdleSleepTimerLocked() {
        if (idleSleepTimerId != 0 && vertx != null) {
            vertx.cancelTimer(idleSleepTimerId);
            idleSleepTimerId = 0;
        }
    }

    private Future<Void> initDownloadStatusReconciliation() {
        // Set up periodic timer to reconcile download statuses every 30 seconds
        if (downloadStatusReconciliationTimerId == 0) {
            downloadStatusReconciliationTimerId = vertx.setPeriodic(30000, _ ->
                    reconcileDownloadStatuses().onFailure(error ->
                            log.error("[%s] Download status reconciliation failed: %s"
                                    .formatted(getRootId(), error.getMessage()))));
            log.debug("[%s] Download status reconciliation timer initialized".formatted(getRootId()));
        }
        return Future.succeededFuture();
    }

    Future<Void> reconcileDownloadStatuses() {
        if (!authorized || telegramRecord == null) {
            return Future.<Void>succeededFuture();
        }

        log.trace("[%s] Starting download status reconciliation".formatted(getRootId()));

        return DataVerticle.fileRepository
                .getByDownloadStatus(telegramRecord.id(), FileRecord.DownloadStatus.downloading)
                .compose(fileRecords -> {
                    if (fileRecords == null || fileRecords.isEmpty()) {
                        return Future.<Void>succeededFuture();
                    }

                    log.debug("[%s] Reconciling %d files with 'downloading' status".formatted(getRootId(), fileRecords.size()));
                    AtomicInteger completedCount = new AtomicInteger();
                    AtomicInteger requeuedCount = new AtomicInteger();
                    AtomicInteger releasedCount = new AtomicInteger();
                    List<Future<Void>> reconciliations = fileRecords.stream()
                            .map(fileRecord -> reconcileDownloadStatus(
                                    fileRecord, completedCount, requeuedCount, releasedCount))
                            .toList();
                    return Future.all(reconciliations)
                            .<Void>mapEmpty()
                            .onSuccess(_ -> {
                                if (completedCount.get() > 0 || requeuedCount.get() > 0 || releasedCount.get() > 0) {
                                    log.info("[%s] Reconciliation completed: completed=%d, requeued=%d, released=%d"
                                            .formatted(getRootId(), completedCount.get(), requeuedCount.get(), releasedCount.get()));
                                }
                            });
                })
                .onFailure(e -> log.error("[%s] Failed to get downloading files for reconciliation: %s".formatted(getRootId(), e.getMessage())));
    }

    private Future<Void> reconcileDownloadStatus(FileRecord fileRecord,
                                                 AtomicInteger completedCount,
                                                 AtomicInteger requeuedCount,
                                                 AtomicInteger releasedCount) {
        return client.execute(new TdApi.GetFile(fileRecord.id()))
                .compose(file -> {
                    if (file.local != null && file.local.isDownloadingCompleted) {
                        log.info("[%s] Reconciliation: File completed but not updated in DB: %s"
                                .formatted(getRootId(), fileRecord.uniqueId()));
                        return DataVerticle.fileRepository.updateDownloadStatus(
                                        file.id,
                                        fileRecord.uniqueId(),
                                        file.local.path,
                                        FileRecord.DownloadStatus.completed,
                                        System.currentTimeMillis()
                                )
                                .compose(result -> applyTelegramMessageTimestamp(fileRecord, file.local.path).map(result))
                                .onSuccess(result -> {
                                    completedCount.incrementAndGet();
                                    sendFileStatusHttpEvent(file, result);
                                })
                                .<Void>mapEmpty();
                    }

                    if (file.local != null && file.local.isDownloadingActive) {
                        return Future.<Void>succeededFuture();
                    }

                    long now = System.currentTimeMillis();
                    if (fileRecord.startDate() > 0
                        && now - fileRecord.startDate() < STALE_DOWNLOAD_RETRY_AFTER_MILLIS) {
                        return Future.<Void>succeededFuture();
                    }

                    return DataVerticle.fileRepository
                            .claimStaleDownloadRetry(fileRecord.uniqueId(), fileRecord.startDate(), now)
                            .compose(claimed -> {
                                if (!claimed) {
                                    return Future.<Void>succeededFuture();
                                }
                                return client.execute(new TdApi.AddFileToDownloads(
                                                file.id,
                                                fileRecord.chatId(),
                                                fileRecord.messageId(),
                                                32
                                        ))
                                        .onSuccess(_ -> {
                                            requeuedCount.incrementAndGet();
                                            log.warn("[%s] Reconciliation requeued stale download: %s"
                                                    .formatted(getRootId(), fileRecord.uniqueId()));
                                        })
                                        .<Void>mapEmpty()
                                        .recover(error -> DataVerticle.fileRepository
                                                .releaseStaleDownloadRetry(fileRecord.uniqueId(), now)
                                                .onSuccess(released -> {
                                                    if (released) {
                                                        releasedCount.incrementAndGet();
                                                    }
                                                    log.warn("[%s] Reconciliation released stale download after retry failed: %s - %s"
                                                            .formatted(getRootId(), fileRecord.uniqueId(), error.getMessage()));
                                                })
                                                .<Void>mapEmpty());
                            });
                })
                .recover(error -> {
                    log.trace("[%s] Failed to get file during reconciliation: %s - %s"
                            .formatted(getRootId(), fileRecord.uniqueId(), error.getMessage()));
                    return Future.<Void>succeededFuture();
                });
    }

    private Future<Void> applyTelegramMessageTimestamp(FileRecord fileRecord, String localPath) {
        return FileTimestampService.applyAsync(vertx, fileRecord, localPath)
                .onSuccess(result -> {
                    if (result.changed()) {
                        log.debug("[%s] Applied Telegram message time to file: %s"
                                .formatted(getRootId(), result.path()));
                    }
                })
                .<Void>mapEmpty()
                .recover(error -> {
                    log.warn("[%s] Failed to apply Telegram message time for %s: %s"
                            .formatted(getRootId(), fileRecord.uniqueId(), error.getMessage()));
                    return Future.<Void>succeededFuture();
                });
    }

    private void onConnectionStateUpdated(TdApi.ConnectionState connectionState) {
        this.lastConnectionState = connectionState;
        log.debug("[%s] Connection state: %s".formatted(getRootId(), connectionState.getClass().getSimpleName()));
        if (connectionState.getConstructor() == TdApi.ConnectionStateWaitingForNetwork.CONSTRUCTOR) {
            // Tell TDLib the network is available so it retries connecting instead of waiting indefinitely.
            tdlibClient.execute(new TdApi.SetNetworkType(new TdApi.NetworkTypeOther()), true);
        }
        sendEvent(EventPayload.build(EventPayload.TYPE_CONNECTION, new JsonObject()
                .put("state", connectionStateName(connectionState))));
    }

    private static String connectionStateName(TdApi.ConnectionState state) {
        return switch (state.getConstructor()) {
            case TdApi.ConnectionStateReady.CONSTRUCTOR -> "ready";
            case TdApi.ConnectionStateConnecting.CONSTRUCTOR -> "connecting";
            case TdApi.ConnectionStateConnectingToProxy.CONSTRUCTOR -> "connectingToProxy";
            case TdApi.ConnectionStateUpdating.CONSTRUCTOR -> "updating";
            case TdApi.ConnectionStateWaitingForNetwork.CONSTRUCTOR -> "waitingForNetwork";
            default -> "unknown";
        };
    }

    private void onAuthorizationStateUpdated(TdApi.AuthorizationState authorizationState) {
        log.debug("[%s] Receive authorization state update: %s"
                .formatted(getRootId(), authorizationState.getClass().getSimpleName()));
        this.lastAuthorizationState = authorizationState;
        switch (authorizationState.getConstructor()) {
            case TdApi.AuthorizationStateWaitTdlibParameters.CONSTRUCTOR:
                TdApi.SetTdlibParameters request = new TdApi.SetTdlibParameters();
                request.databaseDirectory = this.rootPath;
                request.useMessageDatabase = true;
                request.useFileDatabase = true;
                request.useChatInfoDatabase = true;
                request.useSecretChats = true;
                request.apiId = Config.TELEGRAM_API_ID;
                request.apiHash = Config.TELEGRAM_API_HASH;
                request.systemLanguageCode = "en";
                request.deviceModel = "Telegram Files";
                request.applicationVersion = Start.VERSION;
                log.trace("[%s] Send SetTdlibParameters: %s".formatted(getRootId(), request));

                tdlibClient.execute(request).onSuccess(this::handleAuthorizationResult);
                break;
            case TdApi.AuthorizationStateWaitPhoneNumber.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitOtherDeviceConfirmation.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitEmailAddress.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitEmailCode.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitCode.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitRegistration.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitPassword.CONSTRUCTOR:
            case TdApi.AuthorizationStateWaitPremiumPurchase.CONSTRUCTOR:
                authorized = false;
                sendEvent(EventPayload.build(EventPayload.TYPE_AUTHORIZATION, authorizationState));
                break;
            case TdApi.AuthorizationStateReady.CONSTRUCTOR:
                authorized = true;
                boolean becameActive;
                synchronized (lifecycleLock) {
                    becameActive = clientLifecycle != ClientLifecycle.STOPPING;
                    if (becameActive) {
                        clientLifecycle = ClientLifecycle.ACTIVE;
                        idleClose = false;
                        permanentClose = false;
                        if (!readyPromise.future().isComplete()) readyPromise.complete();
                    }
                }
                if (!becameActive) {
                    authorized = false;
                    break;
                }
                if (telegramRecord == null) {
                    client.execute(new TdApi.GetMe())
                            .compose(user -> {
                                TelegramRecord record = new TelegramRecord(user.id, user.firstName, this.rootPath, this.proxyName);
                                // Check existence explicitly rather than inferring a duplicate-key from the
                                // exception message (brittle across DB drivers/versions/locales).
                                return DataVerticle.telegramRepository.getById(user.id)
                                        .compose(existing -> {
                                            if (existing == null) {
                                                return DataVerticle.telegramRepository.create(record);
                                            }
                                            // Account already registered (e.g. re-auth into a fresh root):
                                            // take over by cleaning up the stale verticle, then update the record.
                                            return cleanupOldVerticle(existing.rootPath())
                                                    .compose(_ -> DataVerticle.telegramRepository.update(record));
                                        });
                            })
                            .onSuccess(o -> {
                                telegramRecord = o;
                                log.info("[%s] Account <%s> Authorization Ready".formatted(getRootId(), this.telegramRecord.firstName()));
                                reconcileDownloadStatuses();
                            })
                            .onFailure(e -> log.error("[%s] Authorization Ready, but failed to create telegram record: %s".formatted(getRootId(), e.getMessage())));
                } else {
                    log.info("[%s] Account <%s> Authorization Ready".formatted(getRootId(), this.telegramRecord.firstName()));
                    reconcileDownloadStatuses();
                }
                refreshAccountCache();
                sendEvent(EventPayload.build(EventPayload.TYPE_AUTHORIZATION, authorizationState));
                telegramChats.loadMainChatList();
                telegramChats.loadArchivedChatList();
                if (becameActive) scheduleIdleSleep();
                break;
            case TdApi.AuthorizationStateLoggingOut.CONSTRUCTOR:
                authorized = false;
                cachedAccountJson = null;
                sendEvent(EventPayload.build(EventPayload.TYPE_AUTHORIZATION, authorizationState));
                break;
            case TdApi.AuthorizationStateClosing.CONSTRUCTOR:
                authorized = false;
                cachedAccountJson = null;
                break;
            case TdApi.AuthorizationStateClosed.CONSTRUCTOR:
                authorized = false;
                Promise<Void> completedClose;
                boolean restart;
                synchronized (lifecycleLock) {
                    restart = idleClose && wakeAfterClose && !permanentClose;
                    clientLifecycle = idleClose ? ClientLifecycle.SLEEPING : ClientLifecycle.CLOSED;
                    completedClose = closePromise;
                    closePromise = null;
                    idleClose = false;
                    wakeAfterClose = false;
                }
                String accountName = this.telegramRecord != null ? this.telegramRecord.firstName() : this.getRootId();
                log.info("[%s] Account <%s> closed".formatted(this.getRootId(), accountName));
                if (needDelete) {
                    deleteAccountData().onComplete(res -> {
                        if (completedClose != null && !completedClose.future().isComplete()) {
                            if (res.succeeded()) completedClose.complete();
                            else completedClose.fail(res.cause());
                        }
                    });
                } else {
                    if (completedClose != null && !completedClose.future().isComplete()) completedClose.complete();
                }
                if (restart) {
                    vertx.runOnContext(_ -> initializeTelegramGateway());
                }
                break;
            default:
                log.warn("[%s] Unsupported authorization state received:%s".formatted(this.getRootId(), authorizationState));
        }
    }

    private Future<Void> deleteAccountData() {
        return vertx.<Void>executeBlocking(() -> {
            File root = FileUtil.file(this.rootPath);
            if (root.exists()) {
                boolean deleted = FileUtil.del(root);
                if (!deleted && root.exists()) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ignored) {}
                    FileUtil.del(root);
                }
            }
            return null;
        }).compose(_ -> {
            Future<Void> deletion = getId() instanceof Long telegramId
                    ? DataVerticle.telegramRepository.delete(telegramId).mapEmpty()
                    : Future.succeededFuture();
            return deletion
                    .onSuccess(_ -> log.info("[%s] Telegram account deleted".formatted(this.getRootId())))
                    .onFailure(e -> log.error("[%s] Failed to delete telegram record: %s"
                            .formatted(this.getRootId(), e.getMessage())));
        });
    }

    private void onFileUpdated(TdApi.UpdateFile updateFile) {
        log.trace("📃[%s] Receive file update: %s".formatted(getRootId(), updateFile));
        TdApi.File file = updateFile.file;
        if (file != null) {
            if (file.local != null && file.local.isDownloadingCompleted && StrUtil.isNotBlank(file.local.path)) {
                List<Promise<String>> promises = pendingPreviewPromises.remove(file.id);
                if (promises != null) {
                    for (Promise<String> p : promises) {
                        p.tryComplete(file.local.path);
                    }
                }
            }
            List<PendingChunkRead> chunkReads = pendingChunkReads.get(file.id);
            if (chunkReads != null && !chunkReads.isEmpty()) {
                for (PendingChunkRead pending : chunkReads) {
                    client.execute(new TdApi.ReadFilePart(file.id, pending.offset(), pending.count()))
                            .onSuccess(data -> {
                                if (chunkReads.remove(pending)) {
                                    pending.promise().tryComplete(data.data);
                                }
                            });
                }
            }
            enqueueFileStatusUpdate(file);

            boolean completed = file.local != null && file.local.isDownloadingCompleted;
            long now = System.currentTimeMillis();
            Long lastTime = fileLastEventTimes.get(file.id);
            if (completed || lastTime == null || now - lastTime >= 500) {
                sendEvent(EventPayload.build(EventPayload.TYPE_FILE, updateFile));
                if (completed) {
                    fileLastEventTimes.remove(file.id);
                } else {
                    fileLastEventTimes.put(file.id, now);
                }
            }
        }
    }

    private void enqueueFileStatusUpdate(TdApi.File file) {
        if (file.remote == null || StrUtil.isBlank(file.remote.uniqueId)) {
            return;
        }
        String uniqueId = file.remote.uniqueId;
        Future<Void> scheduled;
        synchronized (fileStatusUpdateTails) {
            Future<Void> previous = fileStatusUpdateTails.get(uniqueId);
            Future<Void> ready = previous == null
                    ? Future.succeededFuture()
                    : previous.recover(_ -> Future.succeededFuture());
            scheduled = ready.compose(_ -> persistFileStatusUpdate(file));
            fileStatusUpdateTails.put(uniqueId, scheduled);
        }

        Future<Void> finalScheduled = scheduled;
        scheduled.onComplete(result -> {
            synchronized (fileStatusUpdateTails) {
                fileStatusUpdateTails.remove(uniqueId, finalScheduled);
            }
            if (result.failed()) {
                log.error("[%s] Failed to update file status %s: %s"
                        .formatted(getRootId(), uniqueId, result.cause().getMessage()));
            }
        });
    }

    private Future<Void> persistFileStatusUpdate(TdApi.File file) {
        String localPath = null;
        Long completionDate = null;
        if (file.local != null && file.local.isDownloadingCompleted) {
            localPath = file.local.path;
            completionDate = System.currentTimeMillis();
        }
        String finalLocalPath = localPath;
        Long finalCompletionDate = completionDate;

        return DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                .compose(fileRecord -> {
                    if (fileRecord == null) {
                        return Future.succeededFuture();
                    }
                    if (shouldPreserveCompletedDownload(fileRecord)) {
                        return applyTelegramMessageTimestamp(fileRecord, fileRecord.localPath());
                    }

                    FileRecord.DownloadStatus downloadStatus = TdApiHelp.getDownloadStatus(file);
                    if (downloadStatus == null) {
                        if (file.local != null && file.local.isDownloadingCompleted) {
                            log.debug("[%s] File download completed but getDownloadStatus returned null: %s"
                                    .formatted(getRootId(), file.remote.uniqueId));
                            downloadStatus = FileRecord.DownloadStatus.completed;
                        } else {
                            downloadStatus = FileRecord.DownloadStatus.idle;
                        }
                    }

                    FileRecord.DownloadStatus finalDownloadStatus = downloadStatus;
                    Future<JsonObject> statusUpdate = DataVerticle.fileRepository.updateDownloadStatus(
                            file.id,
                            file.remote.uniqueId,
                            finalLocalPath,
                            finalDownloadStatus,
                            finalCompletionDate
                    );
                    if (finalDownloadStatus == FileRecord.DownloadStatus.completed) {
                        statusUpdate = statusUpdate.compose(result ->
                                applyTelegramMessageTimestamp(fileRecord, finalLocalPath).map(result));
                    }
                    return statusUpdate
                            .onSuccess(result -> sendFileStatusHttpEvent(file, result))
                            .mapEmpty();
                });
    }

    private void onFileDownloadsUpdated(TdApi.UpdateFileDownloads updateFileDownloads) {
        log.trace("[%s] Receive file downloads update: %s".formatted(getRootId(), updateFileDownloads));
        downloadsActive = updateFileDownloads.totalCount > 0
                          && updateFileDownloads.downloadedSize < updateFileDownloads.totalSize;
        if (downloadsActive) cancelIdleSleepTimer();
        else scheduleIdleSleep();
        avgSpeed.update(updateFileDownloads.downloadedSize, System.currentTimeMillis());
        if (lastFileDownloadEventTime == 0 || System.currentTimeMillis() - lastFileDownloadEventTime > 1000) {
            sendEvent(EventPayload.build(EventPayload.TYPE_FILE_DOWNLOAD, updateFileDownloads));
            lastFileDownloadEventTime = System.currentTimeMillis();
        }
    }

    private void onMessageReceived(TdApi.Message message) {
        log.trace("[%s] Receive message: %s".formatted(getRootId(), message));
        if (this.telegramRecord == null) {
            log.trace("[%s] Telegram record is null, can't handle message".formatted(getRootId()));
            return;
        }
        vertx.eventBus().publish(EventEnum.MESSAGE_RECEIVED.address(), JsonObject.of()
                .put("telegramId", telegramRecord.id())
                .put("chatId", message.chatId)
                .put("messageId", message.id)
        );
    }

    private Future<Void> syncFileDownloadStatus(TdApi.File file, TdApi.Message message, TdApi.MessageThreadInfo messageThreadInfo) {
        return DataVerticle.fileRepository
                .getByUniqueId(file.remote.uniqueId)
                .compose(fileRecord -> {
                    if (fileRecord != null) {
                        return DataVerticle.fileRepository.updateDownloadStatus(
                                file.id,
                                file.remote.uniqueId,
                                file.local.path,
                                FileRecord.DownloadStatus.completed,
                                System.currentTimeMillis()
                        );
                    }

                    if (message == null) {
                        return Future.failedFuture("File not found");
                    }

                    fileRecord = TdApiHelp.getFileHandler(message)
                            .orElseThrow(() -> VertxException.noStackTrace("not support message type"))
                            .convertFileRecord(telegramRecord.id())
                            .withThreadInfo(messageThreadInfo);

                    return DataVerticle.fileRepository.create(fileRecord)
                            .compose(_ -> DataVerticle.fileRepository.updateDownloadStatus(
                                    file.id,
                                    file.remote.uniqueId,
                                    file.local.path,
                                    FileRecord.DownloadStatus.completed,
                                    System.currentTimeMillis()
                            ));
                })
                .compose(r -> DataVerticle.fileRepository.getByUniqueId(file.remote.uniqueId)
                        .compose(fileRecord -> fileRecord == null
                                ? Future.succeededFuture(r)
                                : applyTelegramMessageTimestamp(fileRecord, file.local.path).map(r)))
                .compose(r -> {
                    sendFileStatusHttpEvent(file, r);
                    // Reconciliation is idempotent: an empty update means the database already
                    // contains the same completed path and status, not that synchronization failed.
                    return Future.succeededFuture();
                });
    }

    static boolean shouldPreserveCompletedDownload(FileRecord fileRecord) {
        return fileRecord != null
               && fileRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)
               && StrUtil.isNotBlank(fileRecord.localPath())
               && FileUtil.exist(fileRecord.localPath());
    }
}
