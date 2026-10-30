import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // Proxy API calls to the Go server so we avoid CORS issues in dev.
    // In production the Go server serves the built frontend statically.
    proxy: {
      '/api': 'http://localhost:8080',
      '/metrics': 'http://localhost:8080',
    },
  },
})
