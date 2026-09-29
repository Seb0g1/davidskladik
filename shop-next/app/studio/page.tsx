import type { Metadata } from "next";
import StudioClient from "./StudioClient";

export const metadata: Metadata = {
  title: "Студия постов | Magic Vibes",
  robots: { index: false, follow: false },
};

export default function StudioPage() {
  return <StudioClient />;
}
