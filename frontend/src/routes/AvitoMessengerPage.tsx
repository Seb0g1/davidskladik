import { ChatAutopilotPanel } from "../components/ChatAutopilotPanel";
import { ChatsPage } from "./ChatsPage";

/** Avito chats on their own page, with the chat autopilot (Avito, Ozon and Yandex Market chats) above them. */
export function AvitoMessengerPage() {
  return (
    <ChatsPage
      lockMarketplace="avito"
      title="Мессенджер Авито"
      subtitle="Чаты с покупателями Авито. Автоответы отвечают на «оригинал?» и вопросы об аромате, а после сделки благодарят за покупку."
      top={<ChatAutopilotPanel />}
    />
  );
}
