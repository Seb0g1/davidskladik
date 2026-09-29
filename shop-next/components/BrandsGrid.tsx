"use client";
import Link from "next/link";
import { brandHref } from "@/lib/landings";

interface Props {
  brands: { name: string }[];
}

export default function BrandsGrid({ brands }: Props) {
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
      {brands.map(b => (
        <Link key={b.name} prefetch={false} href={brandHref(b.name)} className="brand-chip">
          {b.name}
        </Link>
      ))}
    </div>
  );
}
