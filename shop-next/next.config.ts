import type { NextConfig } from "next";
import path from "path";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  outputFileTracingRoot: path.join(__dirname, "../"),
  // NEXT_PUBLIC_* is inlined at build time: without this default a build run outside pm2
  // (no ecosystem env) points the browser at magicvibes.ru/api/shop, which does not exist → empty catalog.
  env: {
    NEXT_PUBLIC_API_BASE: process.env.NEXT_PUBLIC_API_BASE || "https://davidsklad.ru",
    NEXT_PUBLIC_SITE_URL: process.env.NEXT_PUBLIC_SITE_URL || "https://magicvibes.ru",
  },
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "**.ozon.ru" },
      { protocol: "https", hostname: "**.ozone.ru" },
      { protocol: "https", hostname: "**.ozonusercontent.com" },
      { protocol: "https", hostname: "**.yandex.net" },
      { protocol: "https", hostname: "**.yastatic.net" },
      { protocol: "https", hostname: "davidsklad.ru" },
      { protocol: "https", hostname: "i.ytimg.com" },
      // Telegram CDN — news photos when the API server could not keep a local copy
      { protocol: "https", hostname: "**.telesco.pe" },
    ],
    formats: ["image/avif", "image/webp"],
  },
  async redirects() {
    return [
      { source: "/catalog/:slug", destination: "/catalog?category=:slug", permanent: false },
    ];
  },
  async headers() {
    return [
      {
        source: "/(.*)",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "SAMEORIGIN" },
        ],
      },
    ];
  },
};

export default nextConfig;
