import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Tauri loads static assets from a local directory in production builds.
  // Next.js must be configured for static export to avoid requiring a Node server at runtime.
  output: "export",
  images: {
    // Static export can't use Next.js Image Optimization (it requires a server).
    unoptimized: true,
    remotePatterns: [
      {
        protocol: "http",
        hostname: "127.0.0.1",
        port: "4320",
        pathname: "/**",
      },
      {
        protocol: "http",
        hostname: "localhost",
        port: "4320",
        pathname: "/**",
      },
    ],
  },
};

export default nextConfig;
