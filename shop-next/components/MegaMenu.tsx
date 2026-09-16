"use client";
import { useState, useRef } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";

interface MegaCol { title: string; items: { label: string; href: string }[] }
interface MegaGroup { id: string; label: string; href: string; columns: MegaCol[] }

const GROUPS: MegaGroup[] = [
  {
    id: "perfume", label: "Парфюмерия", href: "/catalog",
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
    id: "makeup", label: "Макияж", href: "/catalog?q=макияж",
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
    id: "care", label: "Уход", href: "/catalog?category=body",
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
    id: "hair", label: "Для волос", href: "/catalog?q=шампунь",
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
    id: "men", label: "Для мужчин", href: "/catalog?q=мужская",
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
    id: "gifts", label: "Подарки", href: "/gift",
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
    id: "home", label: "Для дома", href: "/catalog?category=home",
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

const MEGA_STYLE = `
.mega-tab {
  position: relative; padding: 12px 16px;
  font-size: 12.5px; letter-spacing: 0.07em; text-transform: uppercase;
  color: rgba(245,244,240,0.56); text-decoration: none; white-space: nowrap;
  transition: color 0.25s; background: transparent; border: none;
  font-family: inherit; cursor: pointer;
}
.mega-tab:hover, .mega-tab.active { color: #f5f4f0; }
.mega-tab::after {
  content: ''; position: absolute; bottom: 0; left: 16px; right: 16px;
  height: 1px; background: #c9a25e;
  transform: scaleX(0); transition: transform 0.3s cubic-bezier(0.16,1,0.3,1);
}
.mega-tab:hover::after, .mega-tab.active::after { transform: scaleX(1); }
.mega-dropdown-item {
  display: block; font-size: 13px; color: rgba(245,244,240,0.5);
  text-decoration: none; padding: 5px 0; letter-spacing: 0.03em;
  line-height: 1.5; transition: color 0.2s;
}
.mega-dropdown-item:hover { color: #f5f4f0; }
`;

export default function MegaMenu() {
  const [activeId, setActiveId] = useState<string | null>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathname = usePathname();

  const scheduleClose = () => { closeTimer.current = setTimeout(() => setActiveId(null), 140); };
  const cancelClose = () => { if (closeTimer.current) clearTimeout(closeTimer.current); };
  const openGroup = (id: string) => { cancelClose(); setActiveId(id); };

  const activeGroup = GROUPS.find(g => g.id === activeId);

  return (
    <>
      <style>{MEGA_STYLE}</style>
      <div className="hidden md:block" style={{ position: "relative", borderTop: "1px solid rgba(255,255,255,0.05)" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 0, padding: "0 clamp(10px,2vw,32px)", overflowX: "auto" }}>
          {GROUPS.map(group => (
            <Link
              key={group.id}
              href={group.href}
              className={`mega-tab${pathname.startsWith(group.href.split("?")[0]) && group.href !== "/" ? " active" : ""}`}
              onMouseEnter={() => openGroup(group.id)}
              onMouseLeave={scheduleClose}
            >
              {group.label}
            </Link>
          ))}
          <Link href="/brands" className={`mega-tab${pathname === "/brands" ? " active" : ""}`}>Бренды</Link>
          <Link href="/new"    className={`mega-tab${pathname === "/new"    ? " active" : ""}`}>Новинки</Link>
          <Link href="/find"   className="mega-tab" style={{ color: "#c9a25e" }}>✦ AI-подбор</Link>
        </div>

        {activeGroup && (
          <div
            onMouseEnter={cancelClose}
            onMouseLeave={scheduleClose}
            className="anim-fade-in"
            style={{
              position: "absolute", left: 0, right: 0, top: "100%", zIndex: 50,
              background: "rgba(10,10,10,0.98)",
              backdropFilter: "blur(20px)", WebkitBackdropFilter: "blur(20px)",
              borderBottom: "1px solid rgba(201,162,94,0.18)",
              boxShadow: "0 24px 64px rgba(0,0,0,0.75)",
              animationDuration: "0.18s",
            }}
          >
            <div style={{
              maxWidth: 1240, margin: "0 auto",
              display: "grid",
              gridTemplateColumns: `repeat(${activeGroup.columns.length}, 1fr)`,
              gap: "32px 48px",
              padding: "32px clamp(18px,4vw,56px) 36px",
            }}>
              {activeGroup.columns.map(col => (
                <div key={col.title}>
                  <p style={{ margin: "0 0 14px", fontSize: 9.5, fontWeight: 400, letterSpacing: "0.28em", textTransform: "uppercase", color: "#c9a25e" }}>
                    {col.title}
                  </p>
                  <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                    {col.items.map(item => (
                      <li key={item.href}>
                        <Link href={item.href} className="mega-dropdown-item" onClick={() => setActiveId(null)}>
                          {item.label}
                        </Link>
                      </li>
                    ))}
                  </ul>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </>
  );
}
