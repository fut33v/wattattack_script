import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/app/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8000",
      "/auth": "http://localhost:8000",
      "/logout": "http://localhost:8000"
    }
  },
  build: {
    outDir: "dist",
    emptyOutDir: true
  }
});
