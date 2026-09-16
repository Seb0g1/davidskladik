"use client";
import Link from "next/link";

interface Props {
  brands: { name: string }[];
}

export default function BrandsGrid({ brands }: Props) {
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
      {brands.map(b => (
        <Link key={b.name} href={`/catalog?brand=${encodeURIComponent(b.name)}`} className="brand-chip">
          {b.name}
        </Link>
      ))}
    </div>
  );
}
