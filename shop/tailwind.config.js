/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        apple: {
          black: "#1d1d1f",
          gray: "#6e6e73",
          "gray-light": "#86868b",
          "gray-bg": "#f5f5f7",
          white: "#ffffff",
          blue: "#0071e3",
        },
        violet: {
          50: "#f5f3ff",
          100: "#ede9fe",
          200: "#ddd6fe",
          300: "#c4b5fd",
          400: "#a78bfa",
          500: "#8b5cf6",
          600: "#7c3aed",
          700: "#6d28d9",
          800: "#5b21b6",
          900: "#4c1d95",
        },
        brand: {
          primary: "#7C3AED",
          dark: "#1d1d1f",
        },
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
        display: ["Playfair Display", "Georgia", "serif"],
        serif: ["Playfair Display", "Georgia", "serif"],
      },
      fontSize: {
        "2xs": "0.625rem",
      },
      letterSpacing: {
        tighter: "-0.03em",
        tight: "-0.02em",
      },
      boxShadow: {
        "card": "0 2px 12px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.04)",
        "card-hover": "0 8px 32px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.06)",
        "xl-soft": "0 20px 60px rgba(0,0,0,0.1)",
        "glass": "0 4px 24px rgba(0,0,0,0.08), inset 0 1px 0 rgba(255,255,255,0.6)",
      },
      backdropBlur: {
        xs: "4px",
      },
      borderRadius: {
        "2xl": "16px",
        "3xl": "24px",
      },
      animation: {
        "fade-in": "fadeIn 0.4s ease both",
        "fade-up": "fadeUp 0.5s ease both",
        "scale-in": "scaleIn 0.3s ease both",
      },
      keyframes: {
        fadeIn: {
          "0%": { opacity: "0" },
          "100%": { opacity: "1" },
        },
        fadeUp: {
          "0%": { opacity: "0", transform: "translateY(16px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        scaleIn: {
          "0%": { opacity: "0", transform: "scale(0.95)" },
          "100%": { opacity: "1", transform: "scale(1)" },
        },
      },
    },
  },
  plugins: [],
};
