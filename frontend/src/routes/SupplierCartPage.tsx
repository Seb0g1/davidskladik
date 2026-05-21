import { RefreshCw } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { SupplierCartPanel } from "./OperationsPage";

export function SupplierCartPage() {
  return (
    <>
      <PageHeader
        title="Автокорзина"
        subtitle="Новые заказы Ozon/Yandex превращаются в черновик заявок PriceMaster с ручным подтверждением."
        action={<a className="secondary-action" href="/app/operations"><RefreshCw size={16} /> Операции</a>}
      />
      <SupplierCartPanel />
    </>
  );
}
