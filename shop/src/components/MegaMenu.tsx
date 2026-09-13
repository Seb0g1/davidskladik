import { useState, useRef } from "react";
import { Link, useLocation } from "react-router-dom";

interface MegaCol {
  title: string;
  items: { label: string; to: string }[];
}
interface MegaGroup {
  id: string;
  label: string;
  to: string;
  columns: MegaCol[];
}

const GROUPS: MegaGroup[] = [
  {
    id: "perfume",
    label: "Парфюмерия",
    to: "/catalog",
    columns: [
      {
        title: "Для кого",
        items: [
          { label: "Женская", to: "/catalog?q=женская" },
          { label: "Мужская", to: "/catalog?q=мужская" },
          { label: "Унисекс", to: "/catalog?q=унисекс" },
          { label: "Детская", to: "/catalog?q=детская" },
        ],
      },
      {
        title: "Категории",
        items: [
          { label: "Пробники и отливанты", to: "/catalog?category=testers" },
          { label: "Нишевая", to: "/catalog?q=нишевая" },
          { label: "Элитная", to: "/catalog?q=элитная" },
          { label: "Восточная / Арабская", to: "/catalog?q=арабская" },
          { label: "Миниатюры", to: "/catalog?q=миниатюр" },
          { label: "Наборы", to: "/catalog?category=sets" },
        ],
      },
      {
        title: "Концентрация",
        items: [
          { label: "Парфюмерная вода", to: "/catalog?category=edp" },
          { label: "Туалетная вода", to: "/catalog?category=edt" },
          { label: "Духи", to: "/catalog?category=parfum" },
          { label: "Одеколон", to: "/catalog?category=edc" },
        ],
      },
      {
        title: "Группы",
        items: [
          { label: "Цветочные", to: "/catalog?q=цветочный" },
          { label: "Древесные", to: "/catalog?q=древесный" },
          { label: "Цитрусовые", to: "/catalog?q=цитрусовый" },
          { label: "Мускусные", to: "/catalog?q=мускусный" },
          { label: "Восточные", to: "/catalog?q=восточный" },
          { label: "Свежие", to: "/catalog?q=свежий" },
          { label: "Фужерные", to: "/catalog?q=фужерный" },
          { label: "Шипровые", to: "/catalog?q=шипровый" },
        ],
      },
    ],
  },
  {
    id: "makeup",
    label: "Макияж",
    to: "/catalog?q=макияж",
    columns: [
      {
        title: "Лицо",
        items: [
          { label: "Тональные средства", to: "/catalog?q=тональный" },
          { label: "Пудры", to: "/catalog?q=пудра" },
          { label: "Консилеры", to: "/catalog?q=консилер" },
          { label: "Румяна", to: "/catalog?q=румяна" },
          { label: "Хайлайтеры", to: "/catalog?q=хайлайтер" },
        ],
      },
      {
        title: "Глаза",
        items: [
          { label: "Тени для век", to: "/catalog?q=тени для век" },
          { label: "Подводки", to: "/catalog?q=подводка" },
          { label: "Тушь для ресниц", to: "/catalog?q=тушь для ресниц" },
          { label: "Карандаши для глаз", to: "/catalog?q=карандаш глаза" },
        ],
      },
      {
        title: "Губы",
        items: [
          { label: "Помады", to: "/catalog?q=помада" },
          { label: "Блески для губ", to: "/catalog?q=блеск для губ" },
          { label: "Бальзамы", to: "/catalog?q=бальзам для губ" },
          { label: "Карандаши для губ", to: "/catalog?q=карандаш для губ" },
        ],
      },
      {
        title: "Для ногтей",
        items: [
          { label: "Лаки", to: "/catalog?q=лак для ногтей" },
          { label: "Уход за ногтями", to: "/catalog?q=уход ногти" },
        ],
      },
    ],
  },
  {
    id: "care",
    label: "Уход",
    to: "/catalog?category=body",
    columns: [
      {
        title: "Лицо",
        items: [
          { label: "Очищение", to: "/catalog?q=очищение лицо" },
          { label: "Увлажнение / Питание", to: "/catalog?q=увлажняющий крем" },
          { label: "Маски", to: "/catalog?q=маска для лица" },
          { label: "Сыворотки / Эмульсии", to: "/catalog?q=сыворотка" },
          { label: "Кремы для лица", to: "/catalog?q=крем для лица" },
          { label: "Антивозрастной уход", to: "/catalog?q=антивозрастной" },
        ],
      },
      {
        title: "Тело",
        items: [
          { label: "Гели для душа", to: "/catalog?q=гель для душа" },
          { label: "Кремы для тела", to: "/catalog?category=body" },
          { label: "Скрабы для тела", to: "/catalog?q=скраб" },
          { label: "Дезодоранты", to: "/catalog?category=deo" },
          { label: "Масла для тела", to: "/catalog?q=масло для тела" },
        ],
      },
      {
        title: "Руки и ноги",
        items: [
          { label: "Кремы для рук", to: "/catalog?q=крем для рук" },
          { label: "Кремы для ног", to: "/catalog?q=крем для ног" },
          { label: "Маски для рук", to: "/catalog?q=маска для рук" },
        ],
      },
    ],
  },
  {
    id: "hair",
    label: "Для волос",
    to: "/catalog?q=шампунь",
    columns: [
      {
        title: "Уход",
        items: [
          { label: "Шампуни", to: "/catalog?q=шампунь" },
          { label: "Кондиционеры", to: "/catalog?q=кондиционер для волос" },
          { label: "Маски", to: "/catalog?q=маска для волос" },
          { label: "Масла", to: "/catalog?q=масло для волос" },
          { label: "Сыворотки", to: "/catalog?q=сыворотка для волос" },
          { label: "Бальзамы", to: "/catalog?q=бальзам для волос" },
        ],
      },
      {
        title: "Стайлинг",
        items: [
          { label: "Лак для волос", to: "/catalog?q=лак для волос" },
          { label: "Мусс", to: "/catalog?q=мусс для волос" },
          { label: "Гель", to: "/catalog?q=гель для волос" },
          { label: "Спрей", to: "/catalog?q=спрей для волос" },
          { label: "Воск / Паста", to: "/catalog?q=воск для волос" },
        ],
      },
    ],
  },
  {
    id: "men",
    label: "Для мужчин",
    to: "/catalog?q=мужская",
    columns: [
      {
        title: "Парфюмерия",
        items: [
          { label: "Все мужские ароматы", to: "/catalog?q=мужская" },
          { label: "Парфюмерная вода", to: "/catalog?category=edp" },
          { label: "Туалетная вода", to: "/catalog?category=edt" },
          { label: "Дезодоранты", to: "/catalog?category=deo" },
          { label: "Пробники", to: "/catalog?category=testers" },
        ],
      },
      {
        title: "Уход",
        items: [
          { label: "Уход за кожей", to: "/catalog?q=мужской уход" },
          { label: "Для бритья", to: "/catalog?q=бритьё" },
          { label: "Уход за бородой", to: "/catalog?q=борода" },
          { label: "Уход за волосами", to: "/catalog?q=шампунь мужской" },
        ],
      },
      {
        title: "Нотки",
        items: [
          { label: "Древесные", to: "/catalog?q=древесный мужской" },
          { label: "Мускусные", to: "/catalog?q=мускусный" },
          { label: "Восточные", to: "/catalog?q=восточный" },
          { label: "Свежие", to: "/catalog?q=свежий мужской" },
          { label: "Фужерные", to: "/catalog?q=фужерный" },
        ],
      },
    ],
  },
  {
    id: "gifts",
    label: "Подарки",
    to: "/gift",
    columns: [
      {
        title: "Для неё",
        items: [
          { label: "Женские ароматы", to: "/catalog?q=женская" },
          { label: "Подарочные наборы", to: "/catalog?category=sets" },
          { label: "Уход за собой", to: "/catalog?category=body" },
        ],
      },
      {
        title: "Для него",
        items: [
          { label: "Мужские ароматы", to: "/catalog?q=мужская" },
          { label: "Наборы для мужчин", to: "/catalog?category=sets" },
        ],
      },
      {
        title: "Особые идеи",
        items: [
          { label: "Пробники и отливанты", to: "/catalog?category=testers" },
          { label: "Парфюм в подарок", to: "/guide/gift" },
          { label: "Ароматы для дома", to: "/catalog?category=home" },
          { label: "Все новинки", to: "/new" },
        ],
      },
    ],
  },
  {
    id: "home",
    label: "Для дома",
    to: "/catalog?category=home",
    columns: [
      {
        title: "Ароматы для дома",
        items: [
          { label: "Все товары", to: "/catalog?category=home" },
          { label: "Ароматические свечи", to: "/catalog?q=свеча ароматическая" },
          { label: "Аромадиффузоры", to: "/catalog?q=аромадиффузор" },
          { label: "Ароматизаторы", to: "/catalog?q=ароматизатор комнаты" },
          { label: "Ароматические спреи", to: "/catalog?q=спрей для дома" },
        ],
      },
    ],
  },
];

const MEGA_STYLE = `
.mega-tab {
  position: relative;
  padding: 12px 16px;
  font-size: 12.5px;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: rgba(245,244,240,0.56);
  text-decoration: none;
  white-space: nowrap;
  transition: color 0.25s;
  background: transparent;
  border: none;
  font-family: inherit;
  cursor: pointer;
}
.mega-tab:hover, .mega-tab.active { color: #f5f4f0; }
.mega-tab::after {
  content: '';
  position: absolute;
  bottom: 0; left: 16px; right: 16px;
  height: 1px;
  background: #c9a25e;
  transform: scaleX(0);
  transition: transform 0.3s cubic-bezier(0.16,1,0.3,1);
}
.mega-tab:hover::after, .mega-tab.active::after { transform: scaleX(1); }
.mega-dropdown-item {
  display: block;
  font-size: 13px;
  color: rgba(245,244,240,0.5);
  text-decoration: none;
  padding: 5px 0;
  letter-spacing: 0.03em;
  line-height: 1.5;
  transition: color 0.2s;
}
.mega-dropdown-item:hover { color: #f5f4f0; }
`;

export default function MegaMenu() {
  const [activeId, setActiveId] = useState<string | null>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const location = useLocation();

  const scheduleClose = () => {
    closeTimer.current = setTimeout(() => setActiveId(null), 140);
  };
  const cancelClose = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
  };
  const openGroup = (id: string) => {
    cancelClose();
    setActiveId(id);
  };

  const activeGroup = GROUPS.find(g => g.id === activeId);
  const path = location.pathname;

  return (
    <>
      <style>{MEGA_STYLE}</style>
      <div
        className="hidden md:block"
        style={{ position: "relative", borderTop: "1px solid rgba(255,255,255,0.05)" }}
      >
        {/* Tab bar */}
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: 0,
          padding: "0 clamp(10px, 2vw, 32px)",
          overflowX: "auto",
        }}>
          {GROUPS.map(group => (
            <Link
              key={group.id}
              to={group.to}
              className={`mega-tab${path.startsWith(group.to.split("?")[0]) && group.to !== "/" ? " active" : ""}`}
              onMouseEnter={() => openGroup(group.id)}
              onMouseLeave={scheduleClose}
            >
              {group.label}
            </Link>
          ))}
          <Link
            to="/brands"
            className={`mega-tab${path === "/brands" ? " active" : ""}`}
          >
            Бренды
          </Link>
          <Link
            to="/new"
            className={`mega-tab${path === "/new" ? " active" : ""}`}
          >
            Новинки
          </Link>
          <Link
            to="/find"
            className="mega-tab"
            style={{ color: "#c9a25e" }}
          >
            ✦ AI-подбор
          </Link>
        </div>

        {/* Dropdown panel */}
        {activeGroup && (
          <div
            onMouseEnter={cancelClose}
            onMouseLeave={scheduleClose}
            className="anim-fade-in"
            style={{
              position: "absolute",
              left: 0, right: 0,
              top: "100%",
              zIndex: 50,
              background: "rgba(10,10,10,0.98)",
              backdropFilter: "blur(20px)",
              WebkitBackdropFilter: "blur(20px)",
              borderBottom: "1px solid rgba(201,162,94,0.18)",
              boxShadow: "0 24px 64px rgba(0,0,0,0.75)",
              animationDuration: "0.18s",
            }}
          >
            <div style={{
              maxWidth: 1240,
              margin: "0 auto",
              display: "grid",
              gridTemplateColumns: `repeat(${activeGroup.columns.length}, 1fr)`,
              gap: "32px 48px",
              padding: "32px clamp(18px,4vw,56px) 36px",
            }}>
              {activeGroup.columns.map(col => (
                <div key={col.title}>
                  <p style={{
                    margin: "0 0 14px",
                    fontSize: 9.5,
                    fontWeight: 400,
                    letterSpacing: "0.28em",
                    textTransform: "uppercase",
                    color: "#c9a25e",
                  }}>
                    {col.title}
                  </p>
                  <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                    {col.items.map(item => (
                      <li key={item.to}>
                        <Link
                          to={item.to}
                          className="mega-dropdown-item"
                          onClick={() => setActiveId(null)}
                        >
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
