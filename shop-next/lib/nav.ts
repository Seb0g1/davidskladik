// Site navigation — shared by the header drawer, search overlay and footer.
import { landingHref } from "./landings";
export interface MegaCol { title: string; items: { label: string; href: string }[] }
export interface MegaGroup { id: string; label: string; href: string; shot: string; columns: MegaCol[] }

export const GROUPS: MegaGroup[] = [
  {
    id: "perfume", shot: "/brand/shots/her.jpg?v=3", label: "Парфюмерия", href: "/catalog?view=grid",
    columns: [
      { title: "Для кого", items: [
        { label: "Женская",  href: "/catalog?q=женская" },
        { label: "Мужская",  href: "/catalog?q=мужская" },
        { label: "Унисекс",  href: "/catalog?q=унисекс" },
        { label: "Детская",  href: "/catalog?q=детская" },
      ]},
      { title: "Категории", items: [
        { label: "Пробники и отливанты", href: "/catalog?category=testers" },
        { label: "Нишевая",             href: "/catalog?q=нишевая" },
        { label: "Элитная",             href: "/catalog?q=элитная" },
        { label: "Восточная / Арабская", href: "/catalog?q=арабская" },
        { label: "Миниатюры",           href: "/catalog?q=миниатюр" },
        { label: "Наборы",              href: "/catalog?category=sets" },
      ]},
      { title: "Концентрация", items: [
        { label: "Парфюмерная вода", href: "/catalog?category=edp" },
        { label: "Туалетная вода",   href: "/catalog?category=edt" },
        { label: "Духи",             href: "/catalog?category=parfum" },
        { label: "Одеколон",         href: "/catalog?category=edc" },
      ]},
      { title: "Группы", items: [
        { label: "Цветочные",  href: "/catalog?q=цветочный" },
        { label: "Древесные",  href: "/catalog?q=древесный" },
        { label: "Цитрусовые", href: "/catalog?q=цитрусовый" },
        { label: "Мускусные",  href: "/catalog?q=мускусный" },
        { label: "Восточные",  href: "/catalog?q=восточный" },
        { label: "Свежие",     href: "/catalog?q=свежий" },
        { label: "Фужерные",   href: "/catalog?q=фужерный" },
        { label: "Шипровые",   href: "/catalog?q=шипровый" },
      ]},
    ],
  },
  {
    id: "makeup", shot: "/brand/shots/gift.jpg?v=3", label: "Макияж", href: "/catalog?q=макияж",
    columns: [
      { title: "Лицо", items: [
        { label: "Тональные средства", href: "/catalog?q=тональный" },
        { label: "Пудры",              href: "/catalog?q=пудра" },
        { label: "Консилеры",          href: "/catalog?q=консилер" },
        { label: "Румяна",             href: "/catalog?q=румяна" },
        { label: "Хайлайтеры",         href: "/catalog?q=хайлайтер" },
      ]},
      { title: "Глаза", items: [
        { label: "Тени для век",       href: "/catalog?q=тени для век" },
        { label: "Подводки",           href: "/catalog?q=подводка" },
        { label: "Тушь для ресниц",    href: "/catalog?q=тушь для ресниц" },
        { label: "Карандаши для глаз", href: "/catalog?q=карандаш глаза" },
      ]},
      { title: "Губы", items: [
        { label: "Помады",             href: "/catalog?q=помада" },
        { label: "Блески для губ",     href: "/catalog?q=блеск для губ" },
        { label: "Бальзамы",           href: "/catalog?q=бальзам для губ" },
        { label: "Карандаши для губ",  href: "/catalog?q=карандаш для губ" },
      ]},
      { title: "Для ногтей", items: [
        { label: "Лаки",              href: "/catalog?q=лак для ногтей" },
        { label: "Уход за ногтями",   href: "/catalog?q=уход ногти" },
      ]},
    ],
  },
  {
    id: "care", shot: "/brand/shots/fresh.jpg?v=3", label: "Уход", href: "/catalog?category=body",
    columns: [
      { title: "Лицо", items: [
        { label: "Очищение",              href: "/catalog?q=очищение лицо" },
        { label: "Увлажнение / Питание",  href: "/catalog?q=увлажняющий крем" },
        { label: "Маски",                 href: "/catalog?q=маска для лица" },
        { label: "Сыворотки / Эмульсии", href: "/catalog?q=сыворотка" },
        { label: "Кремы для лица",        href: "/catalog?q=крем для лица" },
        { label: "Антивозрастной уход",   href: "/catalog?q=антивозрастной" },
      ]},
      { title: "Тело", items: [
        { label: "Гели для душа",  href: "/catalog?q=гель для душа" },
        { label: "Кремы для тела", href: "/catalog?category=body" },
        { label: "Скрабы для тела", href: "/catalog?q=скраб" },
        { label: "Дезодоранты",    href: "/catalog?category=deo" },
        { label: "Масла для тела", href: "/catalog?q=масло для тела" },
      ]},
      { title: "Руки и ноги", items: [
        { label: "Кремы для рук", href: "/catalog?q=крем для рук" },
        { label: "Кремы для ног", href: "/catalog?q=крем для ног" },
        { label: "Маски для рук", href: "/catalog?q=маска для рук" },
      ]},
    ],
  },
  {
    id: "hair", shot: "/brand/shots/citrus.jpg?v=3", label: "Для волос", href: "/catalog?q=шампунь",
    columns: [
      { title: "Уход", items: [
        { label: "Шампуни",     href: "/catalog?q=шампунь" },
        { label: "Кондиционеры", href: "/catalog?q=кондиционер для волос" },
        { label: "Маски",       href: "/catalog?q=маска для волос" },
        { label: "Масла",       href: "/catalog?q=масло для волос" },
        { label: "Сыворотки",   href: "/catalog?q=сыворотка для волос" },
        { label: "Бальзамы",    href: "/catalog?q=бальзам для волос" },
      ]},
      { title: "Стайлинг", items: [
        { label: "Лак для волос", href: "/catalog?q=лак для волос" },
        { label: "Мусс",          href: "/catalog?q=мусс для волос" },
        { label: "Гель",          href: "/catalog?q=гель для волос" },
        { label: "Спрей",         href: "/catalog?q=спрей для волос" },
        { label: "Воск / Паста",  href: "/catalog?q=воск для волос" },
      ]},
    ],
  },
  {
    id: "men", shot: "/brand/shots/him.jpg?v=3", label: "Для мужчин", href: "/catalog?q=мужская",
    columns: [
      { title: "Парфюмерия", items: [
        { label: "Все мужские ароматы", href: "/catalog?q=мужская" },
        { label: "Парфюмерная вода",    href: "/catalog?category=edp" },
        { label: "Туалетная вода",      href: "/catalog?category=edt" },
        { label: "Дезодоранты",         href: "/catalog?category=deo" },
        { label: "Пробники",            href: "/catalog?category=testers" },
      ]},
      { title: "Уход", items: [
        { label: "Уход за кожей",       href: "/catalog?q=мужской уход" },
        { label: "Для бритья",          href: "/catalog?q=бритьё" },
        { label: "Уход за бородой",     href: "/catalog?q=борода" },
        { label: "Уход за волосами",    href: "/catalog?q=шампунь мужской" },
      ]},
      { title: "Нотки", items: [
        { label: "Древесные",  href: "/catalog?q=древесный мужской" },
        { label: "Мускусные",  href: "/catalog?q=мускусный" },
        { label: "Восточные",  href: "/catalog?q=восточный" },
        { label: "Свежие",     href: "/catalog?q=свежий мужской" },
        { label: "Фужерные",   href: "/catalog?q=фужерный" },
      ]},
    ],
  },
  {
    id: "gifts", shot: "/brand/shots/gift.jpg?v=3", label: "Подарки", href: "/collections/nabory",
    columns: [
      { title: "Для неё", items: [
        { label: "Женские ароматы",    href: "/catalog?q=женская" },
        { label: "Подарочные наборы",  href: "/catalog?category=sets" },
        { label: "Уход за собой",      href: "/catalog?category=body" },
      ]},
      { title: "Для него", items: [
        { label: "Мужские ароматы",   href: "/catalog?q=мужская" },
        { label: "Наборы для мужчин", href: "/catalog?category=sets" },
      ]},
      { title: "Особые идеи", items: [
        { label: "Пробники и отливанты", href: "/catalog?category=testers" },
        { label: "Парфюм в подарок",     href: "/guide/gift" },
        { label: "Ароматы для дома",     href: "/catalog?category=home" },
        { label: "Все новинки",          href: "/new" },
      ]},
    ],
  },
  {
    id: "home", shot: "/brand/shots/oriental.jpg?v=3", label: "Для дома", href: "/catalog?category=home",
    columns: [
      { title: "Ароматы для дома", items: [
        { label: "Все товары",         href: "/catalog?category=home" },
        { label: "Ароматические свечи", href: "/catalog?q=свеча ароматическая" },
        { label: "Аромадиффузоры",     href: "/catalog?q=аромадиффузор" },
        { label: "Ароматизаторы",      href: "/catalog?q=ароматизатор комнаты" },
        { label: "Ароматические спреи", href: "/catalog?q=спрей для дома" },
      ]},
    ],
  },
];

// catalog filters that have an SEO landing link straight to it (one URL per listing for crawlers)
for (const g of GROUPS) {
  g.href = landingHref(g.href);
  for (const col of g.columns) for (const it of col.items) it.href = landingHref(it.href);
}

/** Plain links shown under the groups in the menu drawer. */
export const EXTRA_LINKS = [
  { label: "Бренды", href: "/brands" },
  { label: "Новинки", href: "/new" },
  { label: "✦ AI-подбор", href: "/find" },
  { label: "Карта ароматов", href: "/world" },
];

export const SERVICE_LINKS = [
  { label: "Доставка и оплата", href: "/delivery" },
  { label: "Гарантия оригинала", href: "/warranty" },
  { label: "Вопросы и ответы", href: "/faq" },
  { label: "Блог", href: "/blog" },
  { label: "Новости", href: "/news" },
];

export const POPULAR_QUERIES = ["Tom Ford", "Montale", "Byredo", "Kilian", "Dior Sauvage", "Chanel", "арабская", "нишевая", "пробники", "подарочный набор"];
