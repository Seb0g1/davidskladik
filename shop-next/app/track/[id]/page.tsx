import type { Metadata } from "next";
import { Suspense } from "react";
import TrackClient from "./TrackClient";

export const metadata: Metadata = {
  title: "Отслеживание заказа",
  robots: { index: false, follow: false },
};

export default async function TrackPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <Suspense fallback={null}><TrackClient id={decodeURIComponent(id)} /></Suspense>;
}
