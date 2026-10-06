import { ImageOff } from "lucide-react";
import { openPhotoLightbox } from "./PhotoLightbox";

// A small preview for lists: Ozon serves resized copies (/wc200/), other hosts give the original.
function thumbUrl(url: string) {
  return url.replace(/(ir\.ozone\.ru\/s3\/[^/]+\/)(?!wc\d+\/)([^/]+\.(?:jpe?g|png|webp))$/i, "$1wc200/$2");
}

/**
 * The ordered product's photo from its marketplace card — the picker compares it with the item in hand.
 * Tap / click opens all photos (Ozon and Маркет) full screen; the caption says where each one comes from.
 */
export function OrderPhoto({ photos = [], sources = [], name, size = "md" }: { photos?: string[]; sources?: string[]; name: string; size?: "md" | "lg" }) {
  const list = photos.filter(Boolean);
  if (!list.length) {
    return (
      <span className={`order-photo is-empty is-${size}`} title="У карточки нет фото">
        <ImageOff size={16} />
      </span>
    );
  }
  const where = [...new Set(sources.filter(Boolean))].join(" + ");
  return (
    <button
      type="button"
      className={`order-photo is-${size}`}
      title={`Фото с ${where || "маркетплейса"} — открыть`}
      onClick={(event) => {
        event.preventDefault();
        event.stopPropagation();
        openPhotoLightbox(list, 0, `${name}${where ? ` · фото ${where}` : ""}`);
      }}
    >
      <img src={thumbUrl(list[0])} alt="" loading="lazy" onError={(e) => { (e.currentTarget as HTMLImageElement).src = list[0]; }} />
      {list.length > 1 ? <span className="order-photo-count">{list.length}</span> : null}
      {sources[0] ? <span className="order-photo-src">{sources[0]}</span> : null}
    </button>
  );
}
