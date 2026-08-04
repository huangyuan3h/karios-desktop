import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  transpilePackages: ['@karios/shared'],
  // Static export required by Docker nginx (and previously by the Tauri WebView).
  // Next.js must produce static assets so no Node server is needed at runtime.
  output: "export",
  images: {
    // Static export can't use Next.js Image Optimization (it requires a server).
    unoptimized: true,
    remotePatterns: [
      {
        protocol: "http",
        hostname: "127.0.0.1",
        port: "4330",
        pathname: "/**",
      },
      {
        protocol: "http",
        hostname: "localhost",
        port: "4330",
        pathname: "/**",
      },
    ],
  },
};

export default nextConfig;
