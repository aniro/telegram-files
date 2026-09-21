import { type TelegramObject } from "@/lib/websocket-types";

export type TelegramAccount = {
  id: string;
  name: string;
  phoneNumber: string;
  avatar?: string;
  status: "active" | "inactive";
  sleeping?: boolean;
  lastAuthorizationState?: TelegramObject;
  proxy?: string;
  rootPath: string;
};

export type TelegramChat = {
  id: string;
  name: string;
  type: "private" | "group" | "channel";
  isForum?: boolean;
  avatar?: string;
  unreadCount?: number;
  lastMessage?: string;
  lastMessageTime?: string;
  auto?: Auto & {
    state: number;
  };
};

export type FileType = "media" | "photo" | "video" | "audio" | "file";
export type DownloadStatus =
  | "idle"
  | "downloading"
  | "paused"
  | "completed"
  | "error";

export type TransferStatus = "idle" | "transferring" | "completed" | "error";
export type ShareAccessScope = "PUBLIC" | "MEMBER_ACCESS" | "OWNER_ONLY";

export type TelegramFile = {
  id: number;
  telegramId: number;
  uniqueId: string;
  messageId: number;
  chatId: number;
  mediaAlbumId?: number | string;
  fileName: string;
  type: FileType;
  mimeType?: string;
  size: number;
  downloadedSize: number;
  thumbnail?: string;
  thumbnailFile?: Thumbnail;
  downloadStatus: DownloadStatus;
  date: number;
  formatDate: string;
  caption: string;
  localPath: string;
  hasSensitiveContent: boolean;
  startDate: number;
  completionDate: number;
  originalDeleted: boolean;
  transferStatus?: TransferStatus;
  extra?: PhotoExtra | VideoExtra;
  tags?: string;
  loaded: boolean;
  threadChatId: number;
  messageThreadId: number;
  hasReply?: boolean;
  reactionCount: number;

  source?: "TELEGRAM" | "SEED";
  acquiredVia?: "TELEGRAM" | "SEED";
  seedResourceId?: string;
  seedAvailable?: boolean;
  sharedByMe?: boolean;
  shareStatus?:
    | "UNSHARED"
    | "PUBLISH_PENDING"
    | "PUBLISHED"
    | "FAILED"
    | string;
  sharedSourceId?: string;
  sharedResourceId?: string;
  shareTitle?: string;
  shareDescription?: string | null;
  shareTags?: string[];
  shareCategory?: string | null;
  shareAccessScope?: ShareAccessScope;
  sharePublicMessageUrl?: string | null;
  shareErrorCode?: string;
  torrentStatus?: string;
  infoHashV1?: string;
  torrentDownloadSpeed?: number;
  torrentUploadSpeed?: number;
  torrentUploadedBytes?: number;
  torrentDownloadedBytes?: number;
  torrentRatio?: number;
  torrentConnectedPeers?: number;
  torrentSeedingSeconds?: number;

  prev?: TelegramFile;
  next?: TelegramFile;
};

export type PhotoExtra = {
  width: number;
  height: number;
  type: string;
};

export type VideoExtra = {
  width: number;
  height: number;
  duration: number;
  mimeType: string;
};

export type Thumbnail = {
  uniqueId: string;
  mimeType: string;
  extra: {
    width: number;
    height: number;
  };
};

export type TDFile = {
  id: number;
  size: number;
  expectedSize: number;
  local?: {
    path: string;
    canBeDownloaded: boolean;
    canBeDeleted: boolean;
    isDownloadingActive: boolean;
    isDownloadingCompleted: boolean;
    downloadOffset: number;
    downloadedPrefixSize: number;
    downloadedSize: number;
  };
  remote: {
    id: number;
    uniqueId: string;
    isUploadingActive: boolean;
    isUploadingCompleted: boolean;
    uploadedSize: number;
  };
};

export type SortFields = "date" | "completion_date" | "size" | "reaction_count";

export type FileFilter = {
  search: string;
  type: FileType | "all";
  downloadStatus?: DownloadStatus;
  transferStatus?: TransferStatus;
  offline: boolean;
  seedOnly: boolean;
  tags: string[];
  dateType?: "sent" | "downloaded";
  dateRange?: [string, string];
  sizeRange?: [number, number];
  sizeUnit?: "KB" | "MB" | "GB";
  sort?: SortFields;
  order?: "asc" | "desc";
  chatId?: string;
};

export type TelegramApiResult = {
  code: string;
};

export const SettingKeys = [
  "uniqueOnly",
  "imageLoadSize",
  "alwaysHide",
  "showSensitiveContent",
  "autoDownloadLimit",
  "thumbnailAutoLoad",
  "autoDownloadTimeLimited",
  "proxys",
  "avgSpeedInterval",
  "tdlibIdleTimeoutMinutes",
  "speedUnits",
  "tags",
  "shareEnabled",
] as const;

export type SettingKey = (typeof SettingKeys)[number];

export type Settings = Record<SettingKey, string>;

export type Proxy = {
  id?: string;
  name: string;
  server: string;
  port: number;
  username: string;
  password: string;
  secret: string;
  type: "http" | "socks5" | "mtproto";
  isEnabled?: boolean;
};

export type Auto = {
  preload: {
    enabled: boolean;
  };
  download: {
    enabled: boolean;
    rule: AutoDownloadRule;
  };
  transfer: {
    enabled: boolean;
    rule: AutoTransferRule;
  };
  archive: {
    enabled: boolean;
    rule: AutoArchiveRule;
  };
};

export const TransferPolices = [
  "DIRECT",
  "GROUP_BY_CHAT",
  "GROUP_BY_TYPE",
  "GROUP_BY_DATE",
  "GROUP_BY_AI",
] as const;
export type TransferPolicy = (typeof TransferPolices)[number];
export const DuplicationPolicies = [
  "OVERWRITE",
  "RENAME",
  "SKIP",
  "HASH",
] as const;
export type DuplicationPolicy = (typeof DuplicationPolicies)[number];

export const TransferModes = ["MOVE", "COPY", "HARDLINK"] as const;
export type TransferMode = (typeof TransferModes)[number];

export type AutoTransferRule = {
  transferHistory: boolean;
  sourceTopicId?: number | string;
  destination: string;
  transferPolicy: TransferPolicy;
  duplicationPolicy: DuplicationPolicy;
  transferMode?: TransferMode;
  useCaptionName?: boolean;
  extra: Record<string, any>;
};

export type AutoDownloadRule = {
  query: string;
  fileTypes: Array<Exclude<FileType, "media">>;
  downloadHistory: boolean;
  downloadCommentFiles: boolean;
  filterExpr: string;
  allowedExtensions?: string;
  deniedExtensions?: string;
  minSize?: number;
  maxSize?: number;
};

export type ArchiveMode = "COPY" | "FORWARD";
export type ArchiveScope = "ALL_MESSAGES" | "MEDIA_ONLY";
export type ArchiveTopicMode = "MERGE" | "PRESERVE";
export type ArchiveInitialSyncMode = "NOW" | "FULL";

export type CaptionReplacement = {
  pattern: string;
  replacement: string;
};

export type AutoArchiveRule = {
  sourceTopicId: number | string;
  targetChatId: number | string;
  targetTopicId: number | string;
  topicMode: ArchiveTopicMode;
  mode: ArchiveMode;
  scope: ArchiveScope;
  fileTypes: Array<Exclude<FileType, "media">>;
  query: string;
  filterExpr: string;
  preserveCaption: boolean;
  disableNotification: boolean;
  strictOrder: boolean;
  recoveryEnabled: boolean;
  initialSyncMode: ArchiveInitialSyncMode;
  minSize?: number;
  maxSize?: number;
  extensions?: string[];
  cleanCaption?: boolean;
  stripLinks?: boolean;
  stripUsernames?: boolean;
  captionReplacements?: CaptionReplacement[];
  captionSuffix?: string;
};

export type TelegramTopic = {
  id: string;
  name: string;
  general: boolean;
  closed: boolean;
  hidden: boolean;
};

export type CloudArchiveRuleOverview = {
  telegramId: string;
  accountName: string;
  sourceChatId: string;
  sourceChatName: string;
  sourceIsForum?: boolean;
  targetChatId: string;
  targetChatName: string;
  targetIsForum?: boolean;
  targetDownloadEnabled: boolean;
  enabled: boolean;
  rule: AutoArchiveRule;
  syncStatus?: "INITIALIZING" | "LIVE" | "RECOVERING" | "ERROR" | "PAUSED";
  syncTopicCount?: number;
  syncScannedCount?: number;
  syncMatchedCount?: number;
  syncQueuedCount?: number;
  syncLastObservedMessageId?: number;
  syncRecoveryTargetMessageId?: number;
  syncRecoveryCursorMessageId?: number;
  syncError?: string;
  lastReconciledAt?: number;
};

export type CloudArchiveStatistics = {
  total: number;
  completed: number;
  skipped: number;
  failed: number;
  pending: number;
};

export type CloudArchiveOverview = {
  statistics: CloudArchiveStatistics;
  rules: CloudArchiveRuleOverview[];
  accountCooldowns?: Record<
    string,
    {
      cooldownUntil: number;
      remainingSeconds: number;
    }
  >;
};

export type CloudArchiveRecord = {
  id: string;
  telegramId: number;
  sourceChatId: number;
  sourceTopicId: number;
  sourceTopicName?: string;
  sourceMessageId: number;
  sourceAlbumId: number;
  sourceChatName: string;
  targetChatId: number;
  targetTopicId: number;
  targetTopicName?: string;
  generalTopic?: boolean;
  targetMessageId?: number;
  targetChatName: string;
  fileUniqueId?: string;
  mode: ArchiveMode;
  topicMode: ArchiveTopicMode;
  status: string;
  historyJobId?: string;
  attemptCount: number;
  lastErrorCode?: string;
  lastErrorMessage?: string;
  createdAt: number;
  updatedAt: number;
};

export type CloudArchiveHistoryJob = {
  id: string;
  telegramId: number;
  sourceChatId: number;
  sourceTopicId: number;
  sourceChatName: string;
  targetChatId: number;
  targetTopicId: number;
  targetChatName: string;
  topicMode: ArchiveTopicMode | "UNKNOWN";
  archiveMode: ArchiveMode | "UNKNOWN";
  status: string;
  scanMode: "ALL" | "LIMIT";
  stage: "DISCOVERING" | "SCANNING" | "DRAINING" | "COMPLETED";
  maxMessages: number;
  dailyLimit: number;
  dailyDate?: string;
  dailyForwardedCount: number;
  fromMessageId: number;
  scannedCount: number;
  matchedCount: number;
  queuedCount: number;
  topicIndex: number;
  topicCount: number;
  currentTopicId: number;
  completionReason?: "HISTORY_END" | "LIMIT_REACHED";
  lastError?: string;
  createdAt: number;
  updatedAt: number;
};

export type LocalOrganizeRuleOverview = {
  telegramId: string;
  accountName: string;
  sourceChatId: string;
  sourceChatName: string;
  enabled: boolean;
  rule: AutoTransferRule;
};

export type LocalOrganizeOverview = {
  rules: LocalOrganizeRuleOverview[];
};

export type AutomationChatOverview = {
  telegramId: string;
  accountName: string;
  chatId: string;
  chatName: string;
  chatType: TelegramChat["type"] | "unknown";
  chatAvatar?: string;
  unreadCount?: number;
  auto: Auto & {
    state: number;
  };
};
