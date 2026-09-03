const messages = {
  "zh-CN": {
    productName: "tRPC-Agent 多租户平台",
    healthTitle: "平台健康状态",
    checking: "正在检查…",
    unavailable: "Admin API 暂时不可用",
    healthy: "运行正常",
    service: "服务",
    adminApi: "Admin API",
    platformVersion: "平台版本",
    sdk: "SDK",
  },
} as const;

export const defaultLocale = "zh-CN" as const;
export type MessageKey = keyof (typeof messages)[typeof defaultLocale];

export function t(key: MessageKey): string {
  return messages[defaultLocale][key];
}
