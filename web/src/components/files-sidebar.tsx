"use client";

import React, { useMemo, useState } from "react";
import { useMaybeTelegramChat } from "@/hooks/use-telegram-chat";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import { useLanguage } from "@/i18n/language-provider";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useRouter } from "next/navigation";
import useSWR from "swr";
import {
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Film,
  Folder,
  Globe,
  HardDrive,
  ImageIcon,
  Layers,
  Search,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { type FileFilter, type TelegramChat } from "@/lib/types";
import { TooltipWrapper } from "@/components/ui/tooltip";

interface FilesSidebarProps {
  filters: FileFilter;
  onFiltersChange: (filters: FileFilter) => void;
  collapsed?: boolean;
  onToggleCollapsed?: () => void;
}

export default function FilesSidebar({
  filters,
  onFiltersChange,
  collapsed = false,
  onToggleCollapsed,
}: FilesSidebarProps) {
  const { t } = useLanguage();
  const router = useRouter();
  const chatContext = useMaybeTelegramChat();
  const { accountId: currentAccountId, getAccounts } = useTelegramAccount();
  const activeAccountId =
    currentAccountId ||
    getAccounts("active")[0]?.id ||
    getAccounts()[0]?.id;

  const [channelMode, setChannelMode] = useState<"cached" | "online">("cached");

  // Fetch online channels for active account
  const { data: onlineChats, isLoading: isOnlineLoading } = useSWR<TelegramChat[]>(
    !chatContext && activeAccountId && channelMode === "online"
      ? `/telegram/${activeAccountId}/chats`
      : null,
  );

  // Fetch locally cached / downloaded channels from database
  const { data: cachedChats, isLoading: isCachedLoading } = useSWR<
    (TelegramChat & { downloadedCount?: number; totalCount?: number })[]
  >(
    !chatContext && channelMode === "cached"
      ? `/files/chats`
      : null,
  );

  const chats = useMemo(() => {
    if (chatContext) return chatContext.chats;
    return channelMode === "online" ? (onlineChats ?? []) : (cachedChats ?? []);
  }, [chatContext, channelMode, onlineChats, cachedChats]);

  const selectedChatId = chatContext ? chatContext.chatId : filters.chatId;
  const isLoading = chatContext
    ? chatContext.isLoading
    : channelMode === "online"
      ? isOnlineLoading
      : isCachedLoading;
  const [search, setSearch] = useState("");

  const handleChatSelect = (targetChatId: string) => {
    if (chatContext) {
      chatContext.handleChatChange(targetChatId);
    } else if (channelMode === "online") {
      if (activeAccountId && targetChatId) {
        router.push(`/accounts?id=${activeAccountId}&chatId=${targetChatId}`);
      }
    } else {
      onFiltersChange({
        ...filters,
        chatId: filters.chatId === targetChatId ? undefined : targetChatId,
      });
    }
  };

  const filteredChats = React.useMemo(() => {
    if (!chats) return [];
    if (!search.trim()) return chats;
    const q = search.toLowerCase().trim();
    return chats.filter(
      (c) =>
        c.name?.toLowerCase().includes(q) || c.id.toString().includes(q),
    );
  }, [chats, search]);

  const quickNavItems = [
    {
      id: "all",
      label: t("All Files"),
      icon: Folder,
      active:
        filters.type === "all" &&
        filters.downloadStatus === undefined,
      onClick: () =>
        onFiltersChange({
          ...filters,
          type: "all",
          downloadStatus: undefined,
        }),
    },
    {
      id: "downloaded",
      label: t("Downloaded on NAS"),
      icon: HardDrive,
      active: filters.downloadStatus === "completed",
      onClick: () =>
        onFiltersChange({
          ...filters,
          downloadStatus:
            filters.downloadStatus === "completed" ? undefined : "completed",
        }),
    },
    {
      id: "videos",
      label: t("Videos"),
      icon: Film,
      active: filters.type === "video",
      onClick: () =>
        onFiltersChange({
          ...filters,
          type: filters.type === "video" ? "all" : "video",
        }),
    },
    {
      id: "photos",
      label: t("Photos"),
      icon: ImageIcon,
      active: filters.type === "photo",
      onClick: () =>
        onFiltersChange({
          ...filters,
          type: filters.type === "photo" ? "all" : "photo",
        }),
    },
    {
      id: "files",
      label: t("Documents"),
      icon: Layers,
      active: filters.type === "file",
      onClick: () =>
        onFiltersChange({
          ...filters,
          type: filters.type === "file" ? "all" : "file",
        }),
    },
  ];

  if (collapsed) {
    return (
      <aside className="flex flex-col items-center gap-4 rounded-xl border bg-card/60 p-2 backdrop-blur-md">
        <Button
          variant="ghost"
          size="icon"
          className="size-8 text-muted-foreground hover:text-foreground"
          onClick={onToggleCollapsed}
          title={t("Expand sidebar")}
        >
          <ChevronRight className="size-4" />
        </Button>
        <div className="flex flex-col gap-1.5">
          {quickNavItems.map((item) => {
            const Icon = item.icon;
            return (
              <TooltipWrapper key={item.id} content={item.label}>
                <Button
                  variant={item.active ? "secondary" : "ghost"}
                  size="icon"
                  className={cn(
                    "size-9",
                    item.active &&
                      "bg-primary/10 text-primary font-medium hover:bg-primary/20",
                  )}
                  onClick={item.onClick}
                >
                  <Icon className="size-4" />
                </Button>
              </TooltipWrapper>
            );
          })}
        </div>
      </aside>
    );
  }

  return (
    <aside className="flex h-full w-64 shrink-0 flex-col gap-3 rounded-xl border bg-card/60 p-3 shadow-sm backdrop-blur-md">
      {/* Header */}
      <div className="flex items-center justify-between px-1">
        <span className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">
          {t("Library & Navigation")}
        </span>
        {onToggleCollapsed && (
          <Button
            variant="ghost"
            size="icon"
            className="size-6 text-muted-foreground hover:text-foreground"
            onClick={onToggleCollapsed}
            title={t("Collapse sidebar")}
          >
            <ChevronLeft className="size-3.5" />
          </Button>
        )}
      </div>

      {/* Quick Nav Categories */}
      <div className="flex flex-col gap-1">
        {quickNavItems.map((item) => {
          const Icon = item.icon;
          return (
            <Button
              key={item.id}
              variant={item.active ? "secondary" : "ghost"}
              className={cn(
                "h-8 w-full justify-start gap-2.5 px-2.5 text-xs font-normal transition",
                item.active &&
                  "bg-primary/10 font-semibold text-primary hover:bg-primary/20",
              )}
              onClick={item.onClick}
            >
              <Icon className="size-3.5 shrink-0" />
              <span className="truncate">{item.label}</span>
              {item.id === "downloaded" && filters.downloadStatus === "completed" && (
                <CheckCircle2 className="ml-auto size-3 text-emerald-500" />
              )}
            </Button>
          );
        })}
      </div>

      {/* Divider */}
      <div className="my-1 border-t border-border/60" />

      {/* Channel Header & Filter */}
      <div className="flex flex-col gap-2">
        {!chatContext && (
          <div className="grid grid-cols-2 gap-1 rounded-lg bg-muted/60 p-1 text-xs">
            <button
              type="button"
              onClick={() => setChannelMode("cached")}
              className={cn(
                "flex items-center justify-center gap-1.5 rounded-md py-1 font-medium transition",
                channelMode === "cached"
                  ? "bg-background text-foreground shadow-xs"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              <HardDrive className="size-3" />
              <span>{t("Local / Cached") || "本地缓存"}</span>
            </button>
            <button
              type="button"
              onClick={() => setChannelMode("online")}
              className={cn(
                "flex items-center justify-center gap-1.5 rounded-md py-1 font-medium transition",
                channelMode === "online"
                  ? "bg-background text-foreground shadow-xs"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              <Globe className="size-3" />
              <span>{t("Online") || "在线频道"}</span>
            </button>
          </div>
        )}

        <div className="flex items-center justify-between px-1">
          <span className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">
            {channelMode === "online"
              ? t("Online Channels") || "在线频道"
              : t("Local Channels") || "本地频道"}{" "}
            ({chats?.length ?? 0})
          </span>
          {selectedChatId && (
            <button
              onClick={() => handleChatSelect("")}
              className="text-[11px] text-primary hover:underline"
            >
              {t("Clear")}
            </button>
          )}
        </div>
        <div className="relative">
          <Search className="absolute left-2.5 top-2.5 size-3 text-muted-foreground" />
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={
              channelMode === "online"
                ? t("Search online channels...") || "搜索在线频道..."
                : t("Search cached channels...") || "搜索本地缓存频道..."
            }
            className="h-8 pl-7 text-xs"
          />
        </div>
      </div>

      {/* Channel Tree List */}
      <div className="no-scrollbar flex-1 overflow-y-auto pr-1.5">
        <div className="flex flex-col gap-1 py-1">
          {isLoading ? (
            <div className="py-6 text-center text-xs text-muted-foreground">
              {t("Loading channels...")}
            </div>
          ) : filteredChats.length === 0 ? (
            <div className="py-6 text-center text-xs text-muted-foreground">
              {channelMode === "online"
                ? t("No online channels found") || "未找到在线频道"
                : t("No cached channels found") || "暂无本地缓存频道"}
            </div>
          ) : (
            filteredChats.map((c) => {
              const isSelected = selectedChatId === c.id;
              const downloadedCount = (c as { downloadedCount?: number }).downloadedCount;
              return (
                <button
                  key={c.id}
                  onClick={() => handleChatSelect(isSelected ? "" : c.id)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded-lg p-1.5 text-left text-xs transition",
                    isSelected
                      ? "bg-primary text-primary-foreground font-medium shadow-sm"
                      : "hover:bg-muted/70 text-foreground/90",
                  )}
                >
                  <Avatar className="size-6 shrink-0 rounded-md">
                    <AvatarImage
                      src={c.avatar ? `data:image/png;base64,${c.avatar}` : undefined}
                    />
                    <AvatarFallback className="text-[10px]">
                      {c.name?.[0] ?? c.id[0]}
                    </AvatarFallback>
                  </Avatar>
                  <div className="min-w-0 flex-1">
                    <div className="truncate font-medium leading-tight">
                      {c.name || `Chat ${c.id}`}
                    </div>
                  </div>
                  {channelMode === "cached" && downloadedCount !== undefined && (
                    <span
                      className={cn(
                        "shrink-0 rounded px-1.5 py-0.5 text-[10px]",
                        isSelected
                          ? "bg-primary-foreground/20 text-primary-foreground"
                          : "bg-muted text-muted-foreground",
                      )}
                    >
                      {downloadedCount}
                    </span>
                  )}
                </button>
              );
            })
          )}
        </div>
      </div>
    </aside>
  );
}
