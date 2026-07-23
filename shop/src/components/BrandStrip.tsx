const BRANDS = [
  "Chanel", "Dior", "Tom Ford", "Hermès", "Byredo",
  "Jo Malone", "Kilian", "Montale", "Creed", "Dolce & Gabbana",
  "Guerlain", "Givenchy", "Prada", "Yves Saint Laurent", "Lancome",
  "Valentino", "Burberry", "Versace", "Hugo Boss", "Calvin Klein",
];

export default function BrandStrip() {
  const doubled = [...BRANDS, ...BRANDS];

  return (
    <section className="overflow-hidden rounded-2xl bg-white border border-gray-100 py-5">
      <div className="text-center mb-4">
        <span className="text-xs font-semibold uppercase tracking-widest text-gray-400">Популярные бренды</span>
      </div>
      <div className="relative">
        {/* Left fade */}
        <div className="absolute left-0 top-0 bottom-0 w-16 bg-gradient-to-r from-white to-transparent z-10 pointer-events-none" />
        {/* Right fade */}
        <div className="absolute right-0 top-0 bottom-0 w-16 bg-gradient-to-l from-white to-transparent z-10 pointer-events-none" />

        <div className="animate-marquee">
          {doubled.map((brand, i) => (
            <span
              key={i}
              className="inline-flex items-center mx-5 text-sm font-semibold text-gray-400 hover:text-violet-600 transition-colors cursor-pointer whitespace-nowrap"
            >
              {brand}
              <span className="ml-5 text-gray-200">·</span>
            </span>
          ))}
        </div>
      </div>
    </section>
  );
}
