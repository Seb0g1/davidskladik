import React from "react";
import { createRoot } from "react-dom/client";
import { MutationCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App } from "./App";
import { toast } from "./lib/toast";
import "./styles.css";
import "./ux.css";

function mutationErrorText(error: unknown) {
  if (error instanceof Error) return error.message;
  return String(error || "Неизвестная ошибка");
}

const queryClient = new QueryClient({
  // Любая неудачная операция (сохранение, отправка, удаление) показывает всплывающее
  // сообщение — даже если страница не выводит ошибку рядом с кнопкой.
  mutationCache: new MutationCache({
    onError: (error, _variables, _context, mutation) => {
      if (mutation.meta?.silent) return;
      if ((error as { status?: number })?.status === 401) return;
      toast.error(mutationErrorText(error));
    },
  }),
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 20_000,
    },
  },
});

createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </React.StrictMode>,
);
