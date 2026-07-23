import { Link } from "react-router-dom";

const BRANDS = ["Chanel", "Dior", "Tom Ford", "Hermès", "Byredo", "Jo Malone", "Kilian", "Montale", "Creed", "D&G"];

export default function BrandStrip() {
  return (
    <section className="bg-white rounded-2xl shadow-card p-6">
      <h2 className="text-lg font-semibold text-gray-700 mb-4 text-center">Популярные бренды</h2>
      <div className="flex flex-wrap justify-center gap-2">
        {BRANDS.map((brand) => (
          <Link
            key={brand}
            to={`/catalog?brand=${encodeURIComponent(brand)}`}
            className="px-4 py-2 border border-gray-200 rounded-xl text-sm font-medium text-gray-600 hover:border-violet-400 hover:text-violet-700 hover:bg-violet-50 transition-all duration-200"
          >
            {brand}
          </Link>
        ))}
        <Link
          to="/catalog"
          className="px-4 py-2 bg-violet-600 text-white rounded-xl text-sm font-medium hover:bg-violet-700 transition-colors"
        >
          Все бренды →
        </Link>
      </div>
    </section>
  );
}
