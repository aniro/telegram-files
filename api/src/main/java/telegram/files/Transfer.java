package telegram.files;

import java.io.File;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.StructuredChatCompletionCreateParams;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.text.StrFormatter;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.json.JsonObject;
import telegram.files.repository.FileRecord;
import telegram.files.repository.SettingAutoRecords;

public abstract class Transfer {

    private static final Log log = LogFactory.get();

    public String destination;

    public TransferPolicy transferPolicy;

    public SettingAutoRecords.TransferMode transferMode;

    public DuplicationPolicy duplicationPolicy;

    public boolean transferHistory;

    public boolean useCaptionName;

    public JsonObject extra;

    public Consumer<TransferStatusUpdated> transferStatusUpdated;

    private FileRecord transferRecord;

    private static final int MAX_CAPTION_NAME_LENGTH = 80;

    // Keep the whole file name under common filesystem limits (~255), with margin for multibyte chars.
    private static final int MAX_FILENAME_LENGTH = 200;

    private static final Pattern ILLEGAL_FILENAME_CHARS = Pattern.compile("[\\\\/:*?\"<>|\\p{Cntrl}]");

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private static final Pattern TRAILING_DOTS_AND_SPACES = Pattern.compile("[. ]+$");

    private static final Pattern WINDOWS_RESERVED_NAME = Pattern.compile(
            "(?i)^(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\\..*)?$");

    public Transfer(SettingAutoRecords.TransferRule transferRule) {
        this.destination = transferRule.destination;
        this.transferPolicy = transferRule.transferPolicy;
        this.transferMode = transferRule.transferMode != null ? transferRule.transferMode : SettingAutoRecords.TransferMode.MOVE;
        this.duplicationPolicy = transferRule.duplicationPolicy;
        this.transferHistory = transferRule.transferHistory;
        this.useCaptionName = transferRule.useCaptionName;
        this.extra = transferRule.extra != null ? transferRule.extra : new JsonObject();
    }

    public static Transfer create(SettingAutoRecords.TransferRule transferRule) {
        return switch (transferRule.transferPolicy) {
            case DIRECT -> new DirectTransfer(transferRule);
            case GROUP_BY_CHAT -> new GroupByChat(transferRule);
            case GROUP_BY_TYPE -> new GroupByType(transferRule);
            case GROUP_BY_DATE -> new GroupByDate(transferRule);
            case GROUP_BY_AI -> new GroupByAI(transferRule);
        };
    }

    public boolean isRuleUpdated(SettingAutoRecords.TransferRule transferRule) {
        return !Objects.equals(this.destination, transferRule.destination)
               || this.transferPolicy != transferRule.transferPolicy
               || this.transferMode != transferRule.transferMode
               || this.duplicationPolicy != transferRule.duplicationPolicy
               || this.transferHistory != transferRule.transferHistory
               || this.useCaptionName != transferRule.useCaptionName
               || !Objects.equals(this.extra, transferRule.extra);
    }

    public void transfer(FileRecord fileRecord) {
        log.debug("Start transfer file {}", fileRecord.id());
        transferRecord = fileRecord;
        transferStatusUpdated.accept(new TransferStatusUpdated(fileRecord, FileRecord.TransferStatus.transferring, null));
        try {
            File originFile = new File(fileRecord.localPath());
            if (!originFile.exists()) {
                log.error("File {} not found: {}", fileRecord.id(), fileRecord.localPath());
                transferStatusUpdated.accept(new TransferStatusUpdated(fileRecord, FileRecord.TransferStatus.error, null));
                return;
            }

            String transferPath = getTransferPath(fileRecord);
            boolean isOverwrite = false;
            if (FileUtil.exist(transferPath)) {
                if (duplicationPolicy == DuplicationPolicy.SKIP) {
                    log.trace("Skip file {}", fileRecord.id());
                    transferStatusUpdated.accept(new TransferStatusUpdated(fileRecord, FileRecord.TransferStatus.idle, null));
                    return;
                }

                if (duplicationPolicy == DuplicationPolicy.OVERWRITE) {
                    log.trace("Overwrite file {}", fileRecord.id());
                    isOverwrite = true;
                }

                if (duplicationPolicy == DuplicationPolicy.RENAME) {
                    transferPath = getUniquePath(transferPath);
                    log.trace("Rename file {} to {}", fileRecord.id(), transferPath);
                }

                if (duplicationPolicy == DuplicationPolicy.HASH) {
                    File targetFile = new File(transferPath);
                    boolean isSame = originFile.length() == targetFile.length()
                            && MessyUtils.compareFilesMD5(originFile, targetFile);
                    if (isSame) {
                        log.trace("File {} is the same as {}", fileRecord.id(), transferPath);
                        if (transferMode == SettingAutoRecords.TransferMode.MOVE) {
                            FileUtil.del(fileRecord.localPath());
                        }
                        applyTelegramMessageTimestamp(fileRecord, transferPath);
                        transferStatusUpdated.accept(new TransferStatusUpdated(fileRecord, FileRecord.TransferStatus.completed, transferPath));
                        return;
                    } else {
                        transferPath = getUniquePath(transferPath);
                        log.trace("Rename file {} to {}", fileRecord.id(), transferPath);
                    }
                }
            }

            Path sourcePath = Path.of(fileRecord.localPath());
            Path destPath = Path.of(transferPath);
            FileUtil.mkdir(destPath.getParent());

            if (transferMode == SettingAutoRecords.TransferMode.HARDLINK) {
                try {
                    if (isOverwrite) {
                        FileUtil.del(destPath);
                    }
                    java.nio.file.Files.createLink(destPath, sourcePath);
                    log.info("Hardlinked file {} to {}", fileRecord.id(), transferPath);
                } catch (Exception linkError) {
                    log.warn("Hardlink failed ({}), falling back to copy for {}", linkError.getMessage(), fileRecord.id());
                    FileUtil.copy(fileRecord.localPath(), transferPath, isOverwrite);
                }
            } else if (transferMode == SettingAutoRecords.TransferMode.COPY) {
                FileUtil.copy(fileRecord.localPath(), transferPath, isOverwrite);
                log.info("Copied file {} to {}", fileRecord.id(), transferPath);
            } else {
                FileUtil.move(sourcePath, destPath, isOverwrite);
                log.info("Moved file {} to {}, duplication policy: {} overwrite: {}", fileRecord.id(), transferPath, duplicationPolicy, isOverwrite);
            }

            applyTelegramMessageTimestamp(fileRecord, transferPath);
            transferStatusUpdated.accept(new TransferStatusUpdated(fileRecord, FileRecord.TransferStatus.completed, transferPath));
        } catch (Exception e) {
            log.error(e, "Transfer file {} error", fileRecord.id());
            transferStatusUpdated.accept(new TransferStatusUpdated(fileRecord, FileRecord.TransferStatus.error, null));
        } finally {
            transferRecord = null;
        }
    }

    private void applyTelegramMessageTimestamp(FileRecord fileRecord, String localPath) {
        try {
            FileTimestampService.Result result = FileTimestampService.applyBlocking(fileRecord, localPath);
            if (result.changed()) {
                log.debug("Applied Telegram message time after transfer: {}", result.path());
            }
        } catch (Exception error) {
            // Timestamp metadata must never turn a successful file transfer into a failed transfer.
            log.warn("Failed to apply Telegram message time after transfer for {}: {}",
                    fileRecord.uniqueId(), error.getMessage());
        }
    }

    private String getUniquePath(String path) {
        if (!FileUtil.exist(path)) {
            return path;
        }
        String name = FileUtil.getName(path);
        String parent = FileUtil.getParent(path, 1);
        String extension = FileUtil.extName(name);
        String baseName = FileUtil.mainName(name);
        int i = 1;
        while (FileUtil.exists(Path.of(parent, "%s-%d.%s".formatted(baseName, i, extension)), false)) {
            i++;
        }
        return Path.of(parent, "%s-%d.%s".formatted(baseName, i, extension)).toString();
    }

    /**
     * Builds the destination file name. When {@link #useCaptionName} is enabled and the file has a
     * caption, the caption is appended before the extension: {@code <originalBaseName>_<caption>.<ext>},
     * capped at {@link #MAX_FILENAME_LENGTH}. Falls back to the original file name when disabled or
     * when there is no usable caption.
     * <p>Note: the GROUP_BY_AI policy may instead use a file name chosen by the model (when it returns
     * a path that already includes a file name); in that case the caption is not appended.
     */
    protected String buildFileName(FileRecord fileRecord) {
        String originalName = FileUtil.getName(fileRecord.localPath());
        if (!useCaptionName || StrUtil.isBlank(fileRecord.caption())) {
            return originalName;
        }
        String caption = sanitizeForFileName(fileRecord.caption());
        if (StrUtil.isBlank(caption)) {
            return originalName;
        }
        String extension = FileUtil.extName(originalName);
        String baseName = FileUtil.mainName(originalName);
        String stem = "%s_%s".formatted(baseName, caption);
        String suffix = StrUtil.isBlank(extension) ? "" : "." + extension;
        // Cap the whole file name (preserving the extension) so a long original name plus the
        // caption can't exceed common filesystem limits.
        int maxStem = MAX_FILENAME_LENGTH - suffix.length();
        if (maxStem > 0 && stem.codePointCount(0, stem.length()) > maxStem) {
            stem = stem.substring(0, stem.offsetByCodePoints(0, maxStem)).trim();
        }
        return stem + suffix;
    }

    private static String sanitizeForFileName(String text) {
        if (text == null) {
            return "";
        }
        // Normalize equivalent Unicode forms, replace illegal characters, and collapse whitespace.
        String sanitized = WHITESPACE.matcher(ILLEGAL_FILENAME_CHARS.matcher(Normalizer.normalize(text, Normalizer.Form.NFC)).replaceAll(" "))
                .replaceAll(" ")
                .trim();
        sanitized = TRAILING_DOTS_AND_SPACES.matcher(sanitized).replaceAll("");
        if (sanitized.equals(".") || sanitized.equals("..")) {
            return "";
        }
        if (WINDOWS_RESERVED_NAME.matcher(sanitized).matches()) {
            sanitized = "_" + sanitized;
        }
        int codePointCount = sanitized.codePointCount(0, sanitized.length());
        if (codePointCount > MAX_CAPTION_NAME_LENGTH) {
            sanitized = sanitized.substring(0,
                    sanitized.offsetByCodePoints(0, MAX_CAPTION_NAME_LENGTH)).trim();
        }
        return TRAILING_DOTS_AND_SPACES.matcher(sanitized).replaceAll("");
    }

    public FileRecord getTransferRecord() {
        return transferRecord;
    }

    protected abstract String getTransferPath(FileRecord fileRecord);

    /** Calculates the destination without changing the file system. */
    public String previewPath(FileRecord fileRecord) {
        return getTransferPath(fileRecord);
    }

    static class GroupByChat extends Transfer {

        public GroupByChat(SettingAutoRecords.TransferRule transferRule) {
            super(transferRule);
        }

        @Override
        protected String getTransferPath(FileRecord fileRecord) {
            String name = buildFileName(fileRecord);
            return Path.of(destination,
                    Convert.toStr(fileRecord.telegramId()),
                    Convert.toStr(fileRecord.chatId()),
                    name
            ).toString();
        }
    }

    static class GroupByType extends Transfer {

        public GroupByType(SettingAutoRecords.TransferRule transferRule) {
            super(transferRule);
        }

        @Override
        protected String getTransferPath(FileRecord fileRecord) {
            String name = buildFileName(fileRecord);
            return Path.of(destination,
                    fileRecord.type(),
                    name
            ).toString();
        }
    }

    static class GroupByDate extends Transfer {

        public GroupByDate(SettingAutoRecords.TransferRule transferRule) {
            super(transferRule);
        }

        @Override
        protected String getTransferPath(FileRecord fileRecord) {
            String name = buildFileName(fileRecord);
            long epochSeconds = fileRecord.date();
            if (epochSeconds <= 0 && fileRecord.completionDate() != null) {
                epochSeconds = fileRecord.completionDate() / 1000L;
            }
            if (epochSeconds <= 0) {
                return Path.of(destination, "unknown-date", name).toString();
            }

            String configuredZone = extra.getString("timezone", "Asia/Shanghai");
            ZoneId zone;
            try {
                zone = ZoneId.of(configuredZone);
            } catch (ZoneRulesException exception) {
                zone = ZoneId.of("Asia/Shanghai");
            }
            ZonedDateTime messageDate = Instant.ofEpochSecond(epochSeconds).atZone(zone);
            String grouping = extra.getString("dateGrouping", "YEAR_MONTH");

            List<String> parts = new ArrayList<>();
            if (extra.getBoolean("includeChatDirectory", false)) {
                parts.add(Convert.toStr(fileRecord.chatId()));
            }
            parts.add("%04d".formatted(messageDate.getYear()));
            if (!"YEAR".equals(grouping)) {
                parts.add("%02d".formatted(messageDate.getMonthValue()));
            }
            if ("YEAR_MONTH_DAY".equals(grouping)) {
                parts.add("%02d".formatted(messageDate.getDayOfMonth()));
            }
            parts.add(name);
            return Path.of(destination, parts.toArray(String[]::new)).toString();
        }
    }

    static class GroupByAI extends Transfer {
        private final OpenAIClient client;

        private final String promptTemplate;

        public GroupByAI(SettingAutoRecords.TransferRule transferRule) {
            super(transferRule);
            promptTemplate = extra.getString("promptTemplate");
            if (StrUtil.isBlank(promptTemplate)) {
                throw new IllegalArgumentException("Prompt template is required for AI classification transfer policy");
            }
            client = OpenAIOkHttpClient.fromEnv();
        }

        @Override
        protected String getTransferPath(FileRecord fileRecord) {
            Map<String, Object> fileMap = FileRecord.toMap(fileRecord);
            String prompt = StrFormatter.format(promptTemplate, fileMap, false);
            StructuredChatCompletionCreateParams<AIClassificationResult> createParams = ChatCompletionCreateParams.builder()
                    .model(Config.OPENAI_MODEL)
                    .responseFormat(AIClassificationResult.class)
                    .addUserMessage(prompt)
                    .build();

            AIClassificationResult result = client.chat().completions().create(createParams).choices().stream()
                    .flatMap(choice -> choice.message().content().stream())
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No classification result from AI"));
            if (StrUtil.isBlank(result.path)) {
                throw new IllegalStateException("Invalid classification result from AI: " + result);
            }
            log.debug("File {} classified to {} by AI, reason: {}", fileRecord.id(), result.path, result.reason);
            String name = buildFileName(fileRecord);
            // If the model returned a path that already includes a file name (has an extension),
            // that AI-chosen name is used as-is — the caption is intentionally not appended here.
            if (StrUtil.isNotBlank(FileUtil.extName(result.path))) {
                name = "";
            }

            return Path.of(destination,
                    result.path,
                    name
            ).toString();
        }

    }

    static class DirectTransfer extends Transfer {

        public DirectTransfer(SettingAutoRecords.TransferRule transferRule) {
            super(transferRule);
        }

        @Override
        protected String getTransferPath(FileRecord fileRecord) {
            String name = buildFileName(fileRecord);
            return Path.of(destination, name).toString();
        }
    }

    public record TransferStatusUpdated(FileRecord fileRecord,
                                        FileRecord.TransferStatus transferStatus,
                                        String localPath) {
    }

    public enum TransferPolicy {
        /**
         * Transfer files to the specified destination without grouping
         */
        DIRECT,
        /**
         * Transfer files by chat id
         */
        GROUP_BY_CHAT,
        /**
         * Transfer files by type
         */
        GROUP_BY_TYPE,
        /**
         * Transfer files by their Telegram message date.
         */
        GROUP_BY_DATE,
        /**
         * Transfer files by AI classification
         */
        GROUP_BY_AI,
    }

    public enum DuplicationPolicy {
        /**
         * Overwrite the existing file
         */
        OVERWRITE,
        /**
         * Rename the file with a suffix
         */
        RENAME,
        /**
         * Skip the file
         */
        SKIP,
        /**
         * Calculate the hash of the file and compare with the existing file, if the hash is the same,
         * delete the original file and set the local path to the existing file, otherwise, move the file
         */
        HASH,
    }

    @JsonClassDescription("AI Classification Result")
    static class AIClassificationResult {
        @JsonProperty
        @JsonPropertyDescription("A relative path for classification, e.g., images/nature, documents/work/example.pdf")
        public String path;

        @JsonProperty
        @JsonPropertyDescription("Reason for classification or can't classify")
        public String reason;
    }
}
