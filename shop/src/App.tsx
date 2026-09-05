import { BrowserRouter, Routes, Route, useSearchParams } from "react-router-dom";
import { useEffect } from "react";
import Layout from "./components/Layout";
import HomePage from "./pages/HomePage";
import CatalogPage from "./pages/CatalogPage";
import ProductPage from "./pages/ProductPage";
import CartPage from "./pages/CartPage";
import CheckoutPage from "./pages/CheckoutPage";
import OrderSuccessPage from "./pages/OrderSuccessPage";
import OrderFailedPage from "./pages/OrderFailedPage";
import AccountPage from "./pages/AccountPage";
import BrandsPage from "./pages/BrandsPage";
import NewProductsPage from "./pages/NewProductsPage";
import NewsPage from "./pages/NewsPage";
import SupportChatWidget from "./components/SupportChatWidget";
import PopupPromo from "./components/PopupPromo";
import DeliveryPage from "./pages/DeliveryPage";
import WarrantyPage from "./pages/WarrantyPage";
import GiftPage from "./pages/GiftPage";
import FAQPage from "./pages/FAQPage";
import WomenGuide from "./pages/guides/WomenGuide";
import MenGuide from "./pages/guides/MenGuide";
import GiftGuide from "./pages/guides/GiftGuide";
import OfficeGuide from "./pages/guides/OfficeGuide";
import NotFoundPage from "./pages/NotFoundPage";

function RefCapture() {
  const [params] = useSearchParams();
  useEffect(() => {
    const ref = params.get("ref");
    if (ref && /^[A-Z0-9]{8}$/i.test(ref)) {
      localStorage.setItem("shopRefCode", ref.toUpperCase());
    }
  }, [params]);
  return null;
}

export default function App() {
  return (
    <BrowserRouter>
      <RefCapture />
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<HomePage />} />
          <Route path="/catalog" element={<CatalogPage />} />
          <Route path="/catalog/:category" element={<CatalogPage />} />
          <Route path="/product/:offerId" element={<ProductPage />} />
          <Route path="/cart" element={<CartPage />} />
          <Route path="/checkout" element={<CheckoutPage />} />
          <Route path="/order-success" element={<OrderSuccessPage />} />
          <Route path="/order-failed" element={<OrderFailedPage />} />
          <Route path="/orders" element={<AccountPage />} />
          <Route path="/account" element={<AccountPage />} />
          <Route path="/brands" element={<BrandsPage />} />
          <Route path="/new" element={<NewProductsPage />} />
          <Route path="/news" element={<NewsPage />} />
          <Route path="/delivery" element={<DeliveryPage />} />
          <Route path="/warranty" element={<WarrantyPage />} />
          <Route path="/gift" element={<GiftPage />} />
          <Route path="/faq" element={<FAQPage />} />
          <Route path="/guide/women" element={<WomenGuide />} />
          <Route path="/guide/men" element={<MenGuide />} />
          <Route path="/guide/gift" element={<GiftGuide />} />
          <Route path="/guide/office" element={<OfficeGuide />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
      <SupportChatWidget />
      <PopupPromo />
    </BrowserRouter>
  );
}
