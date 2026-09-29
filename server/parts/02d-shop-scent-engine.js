// AI-подбор аромата (/api/shop/ai-search) без внешнего LLM: из ~13 тыс. товаров с нотами
// (warehouse_products.fragrance_notes) и витринного фида (цены, наличие) строится профиль каждого
// аромата; свободный текст разбирается словарями (ноты, аккорды, настроение, сезон, пол, бюджет,
// отрицания, «похожий на …») в вектор предпочтений; товары ранжируются по совпадению
// с объяснением «почему».
// Globals: getPrisma, getShopFeedProducts, cleanText, logger, SHOP_PERFUME_RE, SHOP_NON_PERFUME_RE

// ── Канонизация нот из базы ───────────────────────────────────────────────────
const SCENT_NOTE_ALIASES = {
  "cedarwood": "cedar", "agarwood (oud)": "oud", "agarwood": "oud", "orris": "iris", "orris root": "iris",
  "black currant": "blackcurrant", "cassis": "blackcurrant", "lily-of-the-valley": "lily of the valley", "muguet": "lily of the valley",
  "woodsy notes": "woody notes", "sea notes": "marine", "marine notes": "marine", "aquatic notes": "marine", "sea salt": "salt",
  "frankincense": "incense", "olibanum": "incense", "mandarin orange": "mandarin", "tangerine": "mandarin", "green mandarin": "mandarin",
  "sicilian lemon": "lemon", "calabrian bergamot": "bergamot", "bulgarian rose": "rose", "turkish rose": "rose", "damask rose": "rose",
  "jasmine sambac": "jasmine", "cacao": "cocoa", "white musk": "musk", "clean musk": "musk", "soft musk": "musk",
  "ambroxan": "amber", "ambergris": "amber", "amberwood": "amber", "citrus notes": "citrus", "floral notes": "floral notes",
  "juniper berries": "juniper", "spicy notes": "spices", "lychee": "litchi", "black tea": "tea", "green tea": "tea",
  "fig leaf": "fig", "cashmeran": "cashmere wood", "blood orange": "orange", "bitter orange": "orange", "green apple": "apple",
  "red fruits": "red berries", "pepper": "black pepper", "star anise": "anise", "sichuan pepper": "pink pepper",
};
const scentCanonNote = (n) => { const k = String(n || "").toLowerCase().trim(); return SCENT_NOTE_ALIASES[k] || k; };

// Ярлыки для объяснений (англ. каноническая нота / аккорд → по-русски)
const SCENT_RU = {
  vanilla: "ваниль", amber: "амбра", musk: "мускус", rose: "роза", jasmine: "жасмин", patchouli: "пачули", sandalwood: "сандал",
  "pink pepper": "розовый перец", "black pepper": "чёрный перец", lavender: "лаванда", vetiver: "ветивер", cedar: "кедр", lemon: "лимон",
  "orange blossom": "флёрдоранж", neroli: "нероли", geranium: "герань", cardamom: "кардамон", "green notes": "зелень", saffron: "шафран",
  citrus: "цитрус", mandarin: "мандарин", pear: "груша", peony: "пион", oud: "уд", "woody notes": "дерево", leather: "кожа",
  grapefruit: "грейпфрут", "tonka bean": "бобы тонка", iris: "ирис", spices: "специи", tuberose: "тубероза", ginger: "имбирь",
  violet: "фиалка", incense: "ладан", sage: "шалфей", "clary sage": "шалфей", "ylang-ylang": "иланг-иланг", cinnamon: "корица",
  mint: "мята", rosemary: "розмарин", blackcurrant: "смородина", nutmeg: "мускатный орех", oakmoss: "дубовый мох", apple: "яблоко",
  aldehydes: "альдегиды", peach: "персик", orange: "апельсин", "white flowers": "белые цветы", freesia: "фрезия", tea: "чай",
  benzoin: "бензоин", tobacco: "табак", magnolia: "магнолия", "lily of the valley": "ландыш", heliotrope: "гелиотроп",
  gardenia: "гардения", coconut: "кокос", raspberry: "малина", "cashmere wood": "кашемировое дерево", juniper: "можжевельник",
  basil: "базилик", marine: "морские ноты", cypress: "кипарис", chamomile: "ромашка", lime: "лайм", honey: "мёд", mimosa: "мимоза",
  litchi: "личи", clove: "гвоздика", caramel: "карамель", coffee: "кофе", orchid: "орхидея", lotus: "лотос", plum: "слива",
  almond: "миндаль", rum: "ром", suede: "замша", praline: "пралине", fig: "инжир", cherry: "вишня", cocoa: "какао",
  cucumber: "огурец", honeysuckle: "жимолость", yuzu: "юдзу", apricot: "абрикос", mango: "манго", "red berries": "ягоды",
  pineapple: "ананас", strawberry: "клубника", melon: "дыня", lilac: "сирень", hyacinth: "гиацинт", eucalyptus: "эвкалипт",
  "pine needles": "хвоя", salt: "морская соль", myrrh: "мирра", pomegranate: "гранат", papyrus: "папирус", passionfruit: "маракуйя",
  milk: "молоко", bamboo: "бамбук", "fir resin": "смола пихты", "fir": "пихта", "pine": "сосна", "birch": "берёза", "hay": "сено",
  "orange flower": "цветок апельсина", "sea water": "морская вода", "water notes": "водные ноты", "ozonic notes": "озон", "rice": "рис",
  "guava": "гуава", "vetiver root": "ветивер", "mahogany": "красное дерево", "driftwood": "плавник", "sugar": "сахар", "whiskey": "виски",
  "cognac": "коньяк", "wine": "вино", "champagne": "шампанское", "lemongrass": "лемонграсс", "petitgrain": "петитгрейн", "tonka": "тонка",
  "styrax": "стиракс", "opoponax": "опопонакс", "castoreum": "кастореум", "civet": "цибетин", "cumin": "тмин", "bay leaf": "лавровый лист", labdanum: "лабданум", "guaiac wood": "гваяк", anise: "анис", "floral notes": "цветы",
  Woody: "древесный", Floral: "цветочный", Aromatic: "ароматический", Spicy: "пряный", Musky: "мускусный", Fresh: "свежий",
  Amber: "амбровый", Citrus: "цитрусовый", Sweet: "сладкий", Powdery: "пудровый", Fruity: "фруктовый", Green: "зелёный",
  Oriental: "восточный", "Fresh Spicy": "свежий пряный", Creamy: "кремовый", Clean: "чистый", Leather: "кожаный",
  Aquatic: "морской", Smoky: "дымный", Gourmand: "гурманский", Earthy: "землистый", Herbal: "травяной", "White Floral": "белые цветы",
};
const scentRu = (k) => SCENT_RU[k] || k;

// ── Словарь запроса: основы слов → ноты / аккорды ─────────────────────────────
// s: основы (слово начинается с основы), x: точные слова; n: ноты, a: аккорды (веса), sea: сезон, l: ярлык
const SCENT_TERMS = [
  { s: ["ванил", "vanil"], n: { vanilla: 1 }, a: { Sweet: 0.3, Gourmand: 0.3 }, l: "ваниль" },
  { s: ["амбр", "янтар", "ambe"], n: { amber: 1 }, a: { Amber: 0.8 }, l: "амбра" },
  { s: ["мускус", "musk"], n: { musk: 1 }, a: { Musky: 0.8 }, l: "мускус" },
  { s: ["розов перц", "розовый перец", "розового перца"], n: { "pink pepper": 1 }, l: "розовый перец" },
  { s: ["роз", "rose"], not: ["розов"], n: { rose: 1 }, a: { Floral: 0.4 }, l: "роза" },
  { s: ["жасмин"], n: { jasmine: 1 }, a: { Floral: 0.4, "White Floral": 0.4 }, l: "жасмин" },
  { s: ["пачул"], n: { patchouli: 1 }, a: { Earthy: 0.3, Woody: 0.2 }, l: "пачули" },
  { s: ["сандал"], n: { sandalwood: 1 }, a: { Woody: 0.4, Creamy: 0.3 }, l: "сандал" },
  { s: ["перц", "перец"], n: { "black pepper": 0.8, "pink pepper": 0.6 }, a: { Spicy: 0.5 }, l: "перец" },
  { s: ["лаванд"], n: { lavender: 1 }, a: { Aromatic: 0.5 }, l: "лаванда" },
  { s: ["ветивер"], n: { vetiver: 1 }, a: { Woody: 0.3, Earthy: 0.3 }, l: "ветивер" },
  { s: ["кедр"], n: { cedar: 1 }, a: { Woody: 0.5 }, l: "кедр" },
  { s: ["лимон"], n: { lemon: 1 }, a: { Citrus: 0.6 }, l: "лимон" },
  { s: ["флердоранж", "флёрдоранж", "нерол"], n: { "orange blossom": 1, neroli: 0.8 }, a: { "White Floral": 0.4 }, l: "флёрдоранж" },
  { s: ["герань"], n: { geranium: 1 }, l: "герань" },
  { s: ["кардамон"], n: { cardamom: 1 }, a: { Spicy: 0.4 }, l: "кардамон" },
  { s: ["шафран"], n: { saffron: 1 }, a: { Spicy: 0.3 }, l: "шафран" },
  { s: ["цитрус"], n: { citrus: 0.6, bergamot: 0.5, lemon: 0.4 }, a: { Citrus: 1 }, l: "цитрус" },
  { s: ["бергамот"], n: { bergamot: 1 }, a: { Citrus: 0.4 }, l: "бергамот" },
  { s: ["мандарин", "танжерин"], n: { mandarin: 1 }, a: { Citrus: 0.5 }, l: "мандарин" },
  { s: ["груш"], n: { pear: 1 }, a: { Fruity: 0.5 }, l: "груша" },
  { s: ["пион"], n: { peony: 1 }, a: { Floral: 0.5 }, l: "пион" },
  { x: ["уд", "уда", "удом", "уду", "oud", "oudh"], s: ["агар"], n: { oud: 1 }, a: { Woody: 0.4, Smoky: 0.2 }, l: "уд" },
  { s: ["кож", "замш", "leather"], not: ["кожу", "коже"], n: { leather: 1, suede: 0.6 }, a: { Leather: 1 }, l: "кожа" },
  { s: ["грейпфрут"], n: { grapefruit: 1 }, a: { Citrus: 0.5 }, l: "грейпфрут" },
  { s: ["тонка", "тонко"], n: { "tonka bean": 1 }, a: { Sweet: 0.3 }, l: "бобы тонка" },
  { s: ["ирис", "фиалков"], n: { iris: 1 }, a: { Powdery: 0.5 }, l: "ирис" },
  { s: ["специ", "пряност", "пряны", "пряна", "пряно"], n: { spices: 0.7, cinnamon: 0.4, cardamom: 0.4 }, a: { Spicy: 1 }, l: "пряный" },
  { s: ["тубероз"], n: { tuberose: 1 }, a: { "White Floral": 0.6 }, l: "тубероза" },
  { s: ["имбир"], n: { ginger: 1 }, a: { "Fresh Spicy": 0.5 }, l: "имбирь" },
  { s: ["фиалк"], n: { violet: 1 }, a: { Powdery: 0.3 }, l: "фиалка" },
  { s: ["ладан", "благовон", "смол", "церков"], n: { incense: 1, myrrh: 0.5, labdanum: 0.4 }, a: { Smoky: 0.4, Amber: 0.3 }, l: "ладан" },
  { s: ["шалфе"], n: { sage: 1, "clary sage": 1 }, a: { Aromatic: 0.4 }, l: "шалфей" },
  { s: ["иланг"], n: { "ylang-ylang": 1 }, a: { "White Floral": 0.4 }, l: "иланг-иланг" },
  { s: ["кориц"], n: { cinnamon: 1 }, a: { Spicy: 0.5 }, l: "корица" },
  { s: ["мят"], not: ["мятеж"], n: { mint: 1 }, a: { Fresh: 0.4 }, l: "мята" },
  { s: ["розмарин"], n: { rosemary: 1 }, a: { Aromatic: 0.4 }, l: "розмарин" },
  { s: ["смородин"], n: { blackcurrant: 1 }, a: { Fruity: 0.5 }, l: "смородина" },
  { s: ["мускатн"], n: { nutmeg: 1 }, a: { Spicy: 0.4 }, l: "мускатный орех" },
  { x: ["мох", "мха", "мхом"], s: ["дубов"], n: { oakmoss: 1, moss: 0.8 }, a: { Earthy: 0.3 }, l: "мох" },
  { s: ["яблок", "яблоч"], n: { apple: 1 }, a: { Fruity: 0.5 }, l: "яблоко" },
  { s: ["альдегид"], n: { aldehydes: 1 }, a: { Clean: 0.4 }, l: "альдегиды" },
  { s: ["персик"], n: { peach: 1 }, a: { Fruity: 0.5 }, l: "персик" },
  { s: ["апельсин"], n: { orange: 1 }, a: { Citrus: 0.5 }, l: "апельсин" },
  { s: ["белые цвет", "белых цвет"], n: { "white flowers": 1 }, a: { "White Floral": 1 }, l: "белые цветы" },
  { s: ["фрези"], n: { freesia: 1 }, a: { Floral: 0.4 }, l: "фрезия" },
  { x: ["чай", "чая", "чаем", "чайный", "чайная", "чайные"], n: { tea: 1 }, l: "чай" },
  { s: ["табак", "табач"], n: { tobacco: 1 }, a: { Smoky: 0.3, Sweet: 0.2 }, l: "табак" },
  { s: ["магноли"], n: { magnolia: 1 }, a: { Floral: 0.4 }, l: "магнолия" },
  { s: ["ландыш"], n: { "lily of the valley": 1 }, a: { Floral: 0.4 }, l: "ландыш" },
  { s: ["гардени"], n: { gardenia: 1 }, a: { "White Floral": 0.5 }, l: "гардения" },
  { s: ["кокос"], n: { coconut: 1 }, a: { Creamy: 0.4 }, l: "кокос" },
  { s: ["пудр"], n: { iris: 0.4, violet: 0.3 }, a: { Powdery: 1 }, l: "пудровый" },
  { s: ["малин"], n: { raspberry: 1 }, a: { Fruity: 0.5 }, l: "малина" },
  { s: ["кашемир"], n: { "cashmere wood": 1 }, a: { Woody: 0.3 }, l: "кашемир" },
  { s: ["можжевел", "джин"], n: { juniper: 1 }, a: { Aromatic: 0.3 }, l: "можжевельник" },
  { s: ["базилик"], n: { basil: 1 }, a: { Aromatic: 0.3 }, l: "базилик" },
  { s: ["морск", "море", "моря", "океан", "бриз", "аква"], n: { marine: 1, salt: 0.6 }, a: { Aquatic: 1, Fresh: 0.4 }, l: "морской" },
  { s: ["кипарис"], n: { cypress: 1 }, a: { Woody: 0.3 }, l: "кипарис" },
  { s: ["ромаш"], n: { chamomile: 1 }, l: "ромашка" },
  { s: ["лайм"], n: { lime: 1 }, a: { Citrus: 0.5 }, l: "лайм" },
  { x: ["мед", "мёд", "меда", "мёда", "медом"], s: ["медов", "мёдов"], n: { honey: 1 }, a: { Sweet: 0.5 }, l: "мёд" },
  { s: ["мимоз"], n: { mimosa: 1 }, a: { Powdery: 0.3 }, l: "мимоза" },
  { s: ["личи"], n: { litchi: 1 }, a: { Fruity: 0.4 }, l: "личи" },
  { s: ["гвоздик"], n: { clove: 1 }, a: { Spicy: 0.4 }, l: "гвоздика" },
  { s: ["карамел"], n: { caramel: 1 }, a: { Gourmand: 0.6, Sweet: 0.6 }, l: "карамель" },
  { s: ["кофе", "кофей", "эспрессо"], n: { coffee: 1 }, a: { Gourmand: 0.5 }, l: "кофе" },
  { s: ["орхиде"], n: { orchid: 1 }, a: { Floral: 0.4 }, l: "орхидея" },
  { s: ["лотос"], n: { lotus: 1 }, a: { Floral: 0.3, Aquatic: 0.2 }, l: "лотос" },
  { s: ["слив"], not: ["сливоч", "сливк"], n: { plum: 1 }, a: { Fruity: 0.4 }, l: "слива" },
  { s: ["миндал"], n: { almond: 1 }, a: { Gourmand: 0.4 }, l: "миндаль" },
  { x: ["ром", "рома", "ромом"], s: ["ромов"], n: { rum: 1 }, a: { Sweet: 0.2 }, l: "ром" },
  { s: ["пралине"], n: { praline: 1 }, a: { Gourmand: 0.6 }, l: "пралине" },
  { s: ["инжир", "фиг"], n: { fig: 1 }, a: { Green: 0.3 }, l: "инжир" },
  { s: ["вишн", "черешн", "cherry"], n: { cherry: 1 }, a: { Fruity: 0.5, Sweet: 0.3 }, l: "вишня" },
  { s: ["какао", "шоколад"], n: { cocoa: 1 }, a: { Gourmand: 0.7, Sweet: 0.3 }, l: "шоколад" },
  { s: ["огур"], n: { cucumber: 1 }, a: { Fresh: 0.4, Green: 0.3 }, l: "огурец" },
  { s: ["жимолост"], n: { honeysuckle: 1 }, a: { Floral: 0.3 }, l: "жимолость" },
  { s: ["юдзу", "юзу"], n: { yuzu: 1 }, a: { Citrus: 0.5 }, l: "юдзу" },
  { s: ["абрикос"], n: { apricot: 1 }, a: { Fruity: 0.5 }, l: "абрикос" },
  { s: ["манго"], n: { mango: 1 }, a: { Fruity: 0.6 }, l: "манго" },
  { s: ["молок", "молоч", "сливк", "сливоч", "кремов"], n: { milk: 0.8 }, a: { Creamy: 1 }, l: "сливочный" },
  { s: ["ананас"], n: { pineapple: 1 }, a: { Fruity: 0.6 }, l: "ананас" },
  { s: ["клубник", "земляник"], n: { strawberry: 1 }, a: { Fruity: 0.6, Sweet: 0.3 }, l: "клубника" },
  { x: ["дыня", "дыни", "дыней"], n: { melon: 1 }, a: { Fruity: 0.5 }, l: "дыня" },
  { s: ["сирен"], n: { lilac: 1 }, a: { Floral: 0.4 }, l: "сирень" },
  { s: ["гиацинт"], n: { hyacinth: 1 }, a: { Floral: 0.4 }, l: "гиацинт" },
  { s: ["эвкалипт"], n: { eucalyptus: 1 }, a: { Fresh: 0.4 }, l: "эвкалипт" },
  { s: ["хвой", "хвоя", "хвои", "сосн", "ёлк", "елк", "пихт"], n: { "pine needles": 1 }, a: { Green: 0.4, Woody: 0.3 }, l: "хвоя" },
  { s: ["мирр"], n: { myrrh: 1 }, a: { Amber: 0.3 }, l: "мирра" },
  { s: ["гранат"], n: { pomegranate: 1 }, a: { Fruity: 0.4 }, l: "гранат" },
  { s: ["папирус"], n: { papyrus: 1 }, a: { Woody: 0.3 }, l: "папирус" },
  { s: ["маракуй"], n: { passionfruit: 1 }, a: { Fruity: 0.6 }, l: "маракуйя" },
  { s: ["ягод"], n: { "red berries": 1, raspberry: 0.4, blackcurrant: 0.4 }, a: { Fruity: 0.7 }, l: "ягоды" },
  { s: ["фрукт"], a: { Fruity: 1 }, l: "фруктовый" },
  { s: ["цветоч", "цветы", "цветов", "цветами", "букет", "флорал"], a: { Floral: 1 }, l: "цветочный" },
  { s: ["древес", "дерев", "лесн", "лес"], not: ["лестн"], n: { "woody notes": 0.5, cedar: 0.3, sandalwood: 0.3 }, a: { Woody: 1 }, l: "древесный" },
  { s: ["дым", "копчен", "костр", "костёр", "гарь"], n: { incense: 0.4 }, a: { Smoky: 1 }, l: "дымный" },
  { s: ["землист"], a: { Earthy: 1 }, l: "землистый" },
  { s: ["сладк", "слаще", "сладост"], a: { Sweet: 1, Gourmand: 0.4 }, n: { vanilla: 0.3 }, l: "сладкий" },
  { s: ["гурман", "десерт", "выпечк", "съедоб", "вкусн"], a: { Gourmand: 1, Sweet: 0.5 }, n: { vanilla: 0.3, caramel: 0.3 }, l: "гурманский" },
  { s: ["свеж", "прохлад", "бодрящ", "бодр"], a: { Fresh: 1, Citrus: 0.3 }, l: "свежий" },
  { s: ["мыл", "постиран", "стирк", "бель", "чистот", "чистый", "чистая", "чисто"], n: { musk: 0.5, aldehydes: 0.5 }, a: { Clean: 1, Musky: 0.4, Powdery: 0.2 }, l: "чистый" },
  { s: ["восточ", "ориентал"], a: { Oriental: 1, Amber: 0.6, Spicy: 0.4 }, l: "восточный" },
  { s: ["тепл", "тёпл", "согрева"], a: { Amber: 0.8, Oriental: 0.4, Sweet: 0.2 }, n: { vanilla: 0.3 }, l: "тёплый" },
  { s: ["зелен", "зелён", "трав", "листв", "скошен"], n: { "green notes": 1 }, a: { Green: 1, Herbal: 0.4 }, l: "зелёный" },
  { s: ["фужер", "барбер"], n: { lavender: 0.6, oakmoss: 0.4 }, a: { Aromatic: 1 }, l: "фужерный" },
  { s: ["шипр"], n: { oakmoss: 0.8, patchouli: 0.6, bergamot: 0.5 }, a: { Earthy: 0.3 }, l: "шипровый" },
];

// Настроение, случай, сезон, характер → аккорды и ноты
const SCENT_MOODS = [
  { s: ["офис", "работ", "днев", "учеб", "повседнев", "каждый день", "на каждый"], a: { Fresh: 0.7, Clean: 0.6, Citrus: 0.4, Aromatic: 0.3, Musky: 0.3, Gourmand: -0.4 }, l: "для офиса и каждого дня", light: true },
  { s: ["вечер", "ноч", "свидан", "соблазн", "сексуал", "чувствен", "страст", "ресторан", "клуб", "вечерин", "праздн", "торжеств"], a: { Amber: 0.7, Oriental: 0.6, Spicy: 0.4, Musky: 0.4, Sweet: 0.3, Leather: 0.3 }, n: { vanilla: 0.3, oud: 0.3 }, l: "на вечер" },
  { s: ["летн", "лето", "лета", "летом", "жар", "зной", "отпуск", "пляж", "курорт"], a: { Fresh: 0.8, Citrus: 0.7, Aquatic: 0.6, Green: 0.3, Gourmand: -0.3 }, season: "summer", l: "летний" },
  { s: ["зим", "холод", "мороз", "новогод", "новый год", "рождеств"], a: { Amber: 0.7, Spicy: 0.6, Gourmand: 0.4, Woody: 0.4 }, season: "winter", l: "зимний" },
  { s: ["осен"], a: { Woody: 0.6, Spicy: 0.5, Amber: 0.3 }, season: "fall", l: "осенний" },
  { s: ["весн", "весен"], a: { Floral: 0.7, Green: 0.5, Fresh: 0.4 }, season: "spring", l: "весенний" },
  { s: ["спорт", "трениров", "фитнес"], a: { Fresh: 0.8, Aquatic: 0.5, Citrus: 0.5, Aromatic: 0.3 }, l: "спортивный", light: true },
  { s: ["уют", "домашн", "плед", "обним", "комфорт"], a: { Amber: 0.6, Musky: 0.5, Gourmand: 0.4, Powdery: 0.3 }, n: { vanilla: 0.4 }, l: "уютный" },
  { s: ["брутал", "дерзк", "мужествен", "харизм", "мощн", "уверен"], a: { Leather: 0.6, Woody: 0.6, Smoky: 0.4, Spicy: 0.4 }, l: "дерзкий" },
  { s: ["нежн", "мягк", "романт", "женствен", "утончен", "утончён", "воздуш", "невесом"], a: { Floral: 0.6, Powdery: 0.6, Musky: 0.5 }, l: "нежный" },
  { s: ["элегант", "классич", "классик", "благород", "аристократ", "статус", "роскош", "изыскан"], a: { Woody: 0.4, Amber: 0.4, Powdery: 0.3, Floral: 0.2 }, l: "элегантный" },
  { s: ["загадоч", "мистич", "таинств", "интриг"], a: { Smoky: 0.5, Amber: 0.5, Oriental: 0.5 }, n: { incense: 0.4 }, l: "загадочный" },
  { s: ["молод", "юн", "игрив", "весел", "весёл", "ярк", "позитив", "сочн"], a: { Fruity: 0.6, Sweet: 0.4, Fresh: 0.4 }, l: "яркий" },
];

const SCENT_NEG_WORDS = new Set(["не", "без", "кроме", "никаких", "ни", "нет", "ненавижу", "поменьше", "меньше"]);

// Кириллическое написание брендов и ароматов → как в каталоге
const SCENT_TRANSLIT = [
  ["баккара", "baccarat"], ["бакара", "baccarat"], ["руж", "rouge"], ["шанель", "chanel"], ["коко", "coco"], ["мадмуазель", "mademoiselle"],
  ["шанс", "chance"], ["диор", "dior"], ["саваж", "sauvage"], ["соваж", "sauvage"], ["жадор", "j'adore"], ["мисс диор", "miss dior"],
  ["том форд", "tom ford"], ["лост черри", "lost cherry"], ["табако ваниль", "tobacco vanille"], ["табак ваниль", "tobacco vanille"],
  ["уд вуд", "oud wood"], ["килиан", "kilian"], ["энджелс шер", "angels' share"], ["энджелс", "angels"], ["монталь", "montale"],
  ["латтафа", "lattafa"], ["латафа", "lattafa"], ["хамра", "khamrah"], ["крид", "creed"], ["авентус", "aventus"], ["авентус", "aventus"],
  ["байредо", "byredo"], ["джипси", "gypsy"], ["молекула", "molecule"], ["молекулы", "molecule"], ["эскентрик", "escentric"],
  ["эксцентрик", "escentric"], ["джо малон", "jo malone"], ["ив сен лоран", "yves saint laurent"], ["либре", "libre"],
  ["блэк опиум", "black opium"], ["опиум", "opium"], ["армани", "armani"], ["гуччи", "gucci"], ["флора", "flora"], ["блум", "bloom"],
  ["версаче", "versace"], ["эрос", "eros"], ["брайт кристал", "bright crystal"], ["прада", "prada"], ["парадокс", "paradoxe"],
  ["лакост", "lacoste"], ["хуго", "hugo"], ["хьюго", "hugo"], ["босс", "boss"], ["булгари", "bvlgari"], ["бвлгари", "bvlgari"],
  ["герлен", "guerlain"], ["живанши", "givenchy"], ["ланком", "lancome"], ["ла ви э бель", "la vie est belle"], ["мансера", "mancera"],
  ["марли", "marly"], ["делина", "delina"], ["ксерджофф", "xerjoff"], ["эрба пура", "erba pura"], ["амуаж", "amouage"],
  ["нишане", "nishane"], ["кензо", "kenzo"], ["дольче", "dolce"], ["лайт блю", "light blue"], ["пако рабан", "paco rabanne"],
  ["ван миллион", "1 million"], ["инвиктус", "invictus"], ["фантом", "phantom"], ["готье", "gaultier"], ["ле маль", "le male"],
  ["скандал", "scandal"], ["нарцисо", "narciso"], ["нарцисо родригес", "narciso rodriguez"], ["хлое", "chloe"], ["клоэ", "chloe"],
  ["мюглер", "mugler"], ["мугле", "mugler"], ["ангел", "angel"], ["элиен", "alien"], ["алиен", "alien"], ["эрмес", "hermes"],
  ["терр", "terre"], ["фредерик маль", "frederic malle"], ["ле лабо", "le labo"], ["сантал", "santal"], ["экс нихило", "ex nihilo"],
  ["флер наркотик", "fleur narcotique"], ["кёркджиан", "kurkdjian"], ["куркджан", "kurkdjian"], ["маркиз", "marquis"],
  ["тизиана", "tiziana"], ["терензи", "terenzi"], ["каммарата", "cammarata"], ["аджмал", "ajmal"], ["расаси", "rasasi"],
  ["армаф", "armaf"], ["клаб де нуи", "club de nuit"], ["аль харамайн", "al haramain"], ["маисон", "maison"], ["мейсон", "maison"],
];

// «похож на X», «как X», «аналог X», «вроде X» …
const SCENT_REF_RE = /(?:похож\S*|аналог\S*|напомина\S*|альтернатив\S*|замен\S*|вроде|типа|в стиле|как у|как)\s+(?:на\s+)?(.+?)(?=\s*(?:[,.;!?]|\s(?:но|только|для|чтобы|и|а|до|дешевле|подешевле|на|в)\s|$))/i;

const scentNorm = (s) => String(s || "").toLowerCase().replace(/ё/g, "е").replace(/[’'`]/g, "'").replace(/[^a-zа-я0-9'&\s-]+/g, " ").replace(/\s+/g, " ").trim();

function scentTranslit(text) {
  let t = ` ${scentNorm(text)} `;
  for (const [ru, en] of SCENT_TRANSLIT) t = t.split(` ${scentNorm(ru)} `).join(` ${en} `).split(` ${scentNorm(ru)}`).join(` ${en}`);
  return t.trim();
}

// ── Индекс ароматов: витринный фид + ноты, кеш 30 минут ───────────────────────
let _scentIndex = null;
let _scentIndexAt = 0;
let _scentIndexBuilding = null;
const SCENT_INDEX_TTL = 30 * 60 * 1000;

function scentProductGender(name, notesGender) {
  const n = String(name || "").toLowerCase();
  if (/унисекс|unisex/.test(n)) return "unisex";
  if (/женск|для женщин|pour femme|for women|\bfemme\b|donna/.test(n)) return "female";
  if (/мужск|для мужчин|pour homme|for men|\bhomme\b|uomo/.test(n)) return "male";
  return ["male", "female", "unisex"].includes(notesGender) ? notesGender : "unisex";
}

// «один аромат» независимо от объёма / тестера / концентрации — чтобы не показывать 5 объёмов подряд
function scentIdentity(p) {
  return scentNorm(`${p.brand} ${p.name}`)
    .replace(/\d+([.,]\d+)?\s*(мл|ml)(?=\s|$)/g, " ")
    .replace(/(^|\s)(тестер|tester|парфюмерная|туалетная|вода|парфюмированная|духи|одеколон|eau|de|parfum|toilette|extrait|edp|edt|для|женщин|мужчин|женская|мужская|унисекс|unisex|intense|спрей|spray|refill|миниатюра|mini)(?=\s|$)/g, " ")
    .replace(/(^|\s)(тестер|tester|парфюмерная|туалетная|вода|парфюмированная|духи|одеколон|eau|de|parfum|toilette|extrait|edp|edt|для|женщин|мужчин|женская|мужская|унисекс|unisex|intense|спрей|spray|refill|миниатюра|mini)(?=\s|$)/g, " ")
    .replace(/\s+/g, " ").trim().split(" ").slice(0, 6).join(" ");
}

// «Baccarat Rouge 540» из «Francis Kurkdjian Baccarat Rouge 540 Extrait Духи 200 мл»
function scentShortName(p) {
  let n = String(p.name || "")
    .replace(/\d+([.,]\d+)?\s*(мл|ml)(?=\s|$|[,.)])/gi, " ")
    .replace(/(^|\s)(тестер|tester|парфюмерная|туалетная|парфюмированная|вода|духи|одеколон|eau|de|parfum|toilette|extrait|edp|edt|для|женщин|мужчин|женская|мужская|женские|мужские|женский|мужской|унисекс|unisex|спрей|spray|-)(?=\s|$)/gi, " ")
    .replace(/(^|\s)(тестер|tester|парфюмерная|туалетная|парфюмированная|вода|духи|одеколон|eau|de|parfum|toilette|extrait|edp|edt|для|женщин|мужчин|женская|мужская|женские|мужские|женский|мужской|унисекс|unisex|спрей|spray|-)(?=\s|$)/gi, " ")
    .replace(/\s+/g, " ").trim();
  for (const b of [p.brand, ...String(p.brand || "").split(" ")]) {
    if (b && b.length > 2 && n.toLowerCase().startsWith(b.toLowerCase() + " ")) n = n.slice(b.length).trim();
  }
  n = n.replace(/^(maison\s+)?francis kurkdjian\s+/i, "").replace(/^[-–\s]+/, "");
  return n.split(" ").slice(0, 5).join(" ");
}

async function buildScentIndex() {
  const prisma = getPrisma();
  const feed = await getShopFeedProducts();
  const rows = prisma ? await prisma.warehouseProduct.findMany({
    where: { archived: false, fragranceNotes: { not: null } },
    select: { id: true, fragranceNotes: true },
  }) : [];
  const notesById = new Map(rows.map((r) => [r.id, r.fragranceNotes]));
  const list = [];
  for (const p of feed) {
    if (!p.inStock || !p.priceRub) continue;
    if (!SHOP_PERFUME_RE.test(p.name) || SHOP_NON_PERFUME_RE.test(p.name)) continue;
    const fn = notesById.get(p.id) || null;
    const acc = new Map();
    const notes = new Map();
    if (fn) {
      (Array.isArray(fn.accords) ? fn.accords : []).forEach((a, i) => { if (typeof a === "string") acc.set(a, Math.max(acc.get(a) || 0, [1, 0.85, 0.72, 0.62, 0.55][i] ?? 0.5)); });
      const add = (arr, w) => (Array.isArray(arr) ? arr : []).forEach((n) => { const k = scentCanonNote(n); if (k) notes.set(k, Math.max(notes.get(k) || 0, w)); });
      add(fn.topNotes, 0.7); add(fn.middleNotes, 0.85); add(fn.baseNotes, 1);
    }
    const ml = parseFloat(String(p.volume || "").replace(",", ".")) || 0;
    const lname = p.name.toLowerCase();
    list.push({
      p, acc, notes, hasNotes: !!fn,
      gender: scentProductGender(p.name, fn?.gender),
      seasons: new Set(Array.isArray(fn?.seasons) ? fn.seasons : []),
      ml,
      tester: /тестер|tester/.test(lname),
      set: /набор|set\b|coffret|discovery/.test(lname),
      strong: /extrait|экстракт|духи|parfum\b|elixir|intense|absolu/.test(lname) && !/туалетн|toilette|cologne|одеколон/.test(lname),
      light: /туалетн|toilette|cologne|одеколон|fraiche|fraîche|body mist|вуаль/.test(lname),
      id: scentIdentity(p),
      norm: scentNorm(`${p.brand} ${p.name}`),
      brandN: scentNorm(p.brand),
      pop: Math.log10(1 + (Number(p.reviewCount) || 0)) * ((Number(p.rating) || 4) / 5),
    });
  }
  return list;
}

async function getScentIndex() {
  if (_scentIndex && Date.now() - _scentIndexAt < SCENT_INDEX_TTL) return _scentIndex;
  if (!_scentIndexBuilding) {
    _scentIndexBuilding = buildScentIndex()
      .then((l) => { _scentIndex = l; _scentIndexAt = Date.now(); logger.info("scent index built", { products: l.length, withNotes: l.filter((x) => x.hasNotes).length }); return l; })
      .finally(() => { _scentIndexBuilding = null; });
  }
  return _scentIndex || _scentIndexBuilding;
}

// ── Разбор запроса ────────────────────────────────────────────────────────────
function scentParsePrice(text) {
  const t = text.replace(/(\d)\s+(?=\d{3}\b)/g, "$1"); // «10 000» → «10000»
  const num = (v, unit) => {
    let n = parseFloat(String(v).replace(",", "."));
    if (!Number.isFinite(n)) return 0;
    if (/^(к|k|тыс|т)/.test(unit || "")) n *= 1000;
    else if (n < 100) n *= 1000; // «до 15» в контексте цены — тысячи
    return Math.round(n);
  };
  let min = 0, max = 0;
  const range = t.match(/от\s*(\d+(?:[.,]\d+)?)\s*(к|k|тыс\S*|т\.?\s?р\.?)?\s*(?:₽|руб\S*|р\.?)?\s*до\s*(\d+(?:[.,]\d+)?)\s*(к|k|тыс\S*|т\.?\s?р\.?)?/i);
  if (range) { min = num(range[1], range[2] || range[4]); max = num(range[3], range[4]); }
  const upTo = !range && (t.match(/(?:до|не дороже|дешевле|в пределах|максимум|бюджет[а-я]*)\s*(\d+(?:[.,]\d+)?)(?!\s*(?:мл|ml|%|лет|год|дн|шт|г\b))\s*(к|k|тыс\S*|т\.?\s?р\.?)?/i)
    || t.match(/за\s*(\d+(?:[.,]\d+)?)\s*(к|k|тыс\S*|₽|руб\S*)/i));
  if (upTo) max = num(upTo[1], upTo[2]);
  const from = !range && !upTo && t.match(/(?:от|дороже)\s*(\d+(?:[.,]\d+)?)\s*(к|k|тыс\S*)?\s*(?:₽|руб\S*)/i);
  if (from) min = num(from[1], from[2]);
  const cheap = /недорог|бюджетн|дешев|подешевле|эконом|доступн/.test(t);
  return { min, max, cheap };
}

function scentParse(query, index) {
  const raw = cleanText(query || "").slice(0, 400);
  const text = scentNorm(raw);
  const words = text.split(" ");
  // same words, with clause breaks kept («без цветов, дерево и кожа» — «дерево» не отрицается)
  const clauseWords = scentNorm(raw.replace(/[,.;!?]+/g, " ¦ ").replace(/¦/g, "zzbreak")).split(" ").map((w) => (w === "zzbreak" ? "¦" : w));
  const qA = new Map(), qN = new Map();
  const labels = [], negLabels = [];
  const seasons = new Set();
  let light = false, strong = false;
  const addW = (map, obj, k = 1) => { for (const [key, w] of Object.entries(obj || {})) map.set(key, (map.get(key) || 0) + w * k); };

  // слово i отрицается, если в 1–3 словах до него есть «не/без/кроме…»
  const negated = (wi) => {
    // wi — индекс в words; ищем то же слово в clauseWords и смотрим назад до границы фразы
    let seen = -1, ci = -1;
    for (let k = 0; k < clauseWords.length; k++) { if (clauseWords[k] !== "¦") seen++; if (seen === wi) { ci = k; break; } }
    for (let k = ci - 1, n = 0; k >= 0 && n < 3; k--) {
      const w = clauseWords[k];
      if (w === "¦" || ["и", "а", "но", "зато", "плюс", "или"].includes(w)) return false;
      if (SCENT_NEG_WORDS.has(w)) return true;
      n++;
    }
    return false;
  };
  const matchEntry = (e) => {
    // многословные основы («белые цвет») ищем по тексту, остальные — по словам
    for (const st of e.s || []) {
      if (st.includes(" ")) { const at = text.indexOf(st); if (at >= 0) return text.slice(0, at).split(" ").length - 1; }
    }
    for (let i = 0; i < words.length; i++) {
      const w = words[i];
      if ((e.not || []).some((n) => w.startsWith(n))) continue;
      if ((e.x || []).includes(w)) return i;
      if ((e.s || []).some((st) => !st.includes(" ") && w.startsWith(st))) return i;
    }
    return -1;
  };
  const usedLabels = new Set();
  for (const e of SCENT_TERMS) {
    const i = matchEntry(e);
    if (i < 0 || usedLabels.has(e.l)) continue;
    usedLabels.add(e.l);
    const neg = negated(i);
    addW(qA, e.a, neg ? -1 : 1);
    addW(qN, e.n, neg ? -1 : 1);
    (neg ? negLabels : labels).push(e.l);
  }
  // «приторный», «не слишком сладкий» — против сладости даже без «не»
  if (/приторн|не слишком слад|не очень слад|без сладост|не сладк/.test(text)) {
    qA.set("Sweet", Math.min(qA.get("Sweet") || 0, 0) - 0.8); qA.set("Gourmand", Math.min(qA.get("Gourmand") || 0, 0) - 0.6);
    if (!negLabels.includes("сладкий")) negLabels.push("сладкий");
    const i = labels.indexOf("сладкий"); if (i >= 0) labels.splice(i, 1);
  }
  const moodLabels = [];
  for (const m of SCENT_MOODS) {
    const i = matchEntry(m);
    if (i < 0) continue;
    const neg = negated(i);
    addW(qA, m.a, neg ? -0.6 : 1);
    addW(qN, m.n, neg ? -0.6 : 1);
    if (!neg && m.season) seasons.add(m.season);
    if (!neg && m.light) light = true;
    moodLabels.push(neg ? `не ${m.l}` : m.l);
  }
  if (/стойк|шлейф|долго держ|держал|мощн|насыщен/.test(text)) strong = true;
  if (/легк|лёгк|ненавязчив|тих|незамет|близко к коже|деликатн/.test(text)) light = true;

  // пол
  let gender = "any";
  if (/(^|\s)(мужч|мужск|муж|мужу|мужа|парн|для него|папе|папы|пап[аеу]|отц|брат|сын|дедушк|любимому|жениху|коллеге мужчине)/.test(text)) gender = "male";
  if (/(^|\s)(женщ|женск|девушк|для нее|для неё|мам[аеыу]|маме|сестр|подруг|дочер|жене|жена|жену|любимой|невест|бабушк|теще|свекров)/.test(text)) gender = gender === "male" ? "any" : "female";
  if (/унисекс|для двоих|для обоих/.test(text)) gender = "unisex";

  const price = scentParsePrice(raw.toLowerCase());
  const groups = [];
  if (/нишев|(^|\s)ниш[аиуе](\s|$)|niche/.test(text)) groups.push("нишевая");
  if (/элитн|люкс|премиум|роскошн/.test(text)) groups.push("элитная");
  if (/арабск|(^|\s)араб/.test(text)) groups.push("арабская");
  const sample = /пробник|отливант|миниатюр|попробовать|распив|семпл|сэмпл|затест/.test(text);
  const wantSet = /набор|подарочн\S* набор|коробк/.test(text);
  const gift = /подар/.test(text);

  // бренды и «похожий на …»: ищем по транслиту
  const tl = scentTranslit(raw);
  let reference = null;
  const refM = tl.match(SCENT_REF_RE) || scentNorm(raw).match(SCENT_REF_RE);
  const refText = refM ? scentTranslit(refM[1]) : "";
  const findProduct = (phrase) => {
    const toks = scentNorm(phrase).split(" ").filter((t) => t.length >= 2 && !["на", "как", "у", "и", "the", "de", "мне"].includes(t));
    if (!toks.length) return null;
    let best = null, bestScore = 0;
    for (const it of index) {
      if (it.set || (it.ml && it.ml < 15)) continue;
      const hit = toks.filter((t) => it.norm.includes(t)).length;
      if (!hit) continue;
      const score = hit / toks.length + (it.hasNotes ? 0.05 : 0) + (it.ml >= 50 && it.ml <= 100 ? 0.03 : 0) + (it.tester ? -0.02 : 0) + it.pop * 0.01;
      if (score > bestScore) { bestScore = score; best = it; }
    }
    if (!(bestScore >= 0.99 || (toks.length >= 3 && bestScore >= 0.66))) return null;
    const key = scentNorm(scentShortName(best.p)).split(" ").filter((t) => t.length >= 2);
    const editions = index.filter((it) => !it.set && !(it.ml && it.ml < 15) && key.every((t) => it.norm.includes(t)));
    // базовая версия: без лишних слов к названию (Cologne, Extrait, Elixir…), обычный флакон 30–100 мл, не тестер
    const extra = (it) => scentNorm(scentShortName(it.p)).split(" ").filter((t) => t.length >= 2 && !key.includes(t)).length;
    const rank = (it) => extra(it) * 10 + (it.tester ? 3 : 0) + (it.ml >= 30 && it.ml <= 100 ? 0 : it.ml ? 2 : 1) + it.p.priceRub / 1e6;
    const rep = (editions.length ? editions : [best]).reduce((a, b) => (rank(b) < rank(a) ? b : a));
    const withNotes = [rep, best, ...editions].find((it) => it.hasNotes) || rep;
    return { ...rep, acc: withNotes.acc, notes: withNotes.notes, hasNotes: withNotes.hasNotes, seasons: withNotes.seasons, refKey: key };
  };
  const scentWord = (w) => [...SCENT_TERMS, ...SCENT_MOODS].some((e) => (e.x || []).includes(w) || (e.s || []).some((st) => !st.includes(" ") && w.startsWith(st)));
  const refWords = scentNorm(refM ? refM[1] : "").split(" ").filter((w) => w.length > 1);
  if (refText && refWords.some((w) => !scentWord(w) && !["у", "на", "что", "то", "это", "мне", "бы"].includes(w))) reference = findProduct(refText);

  // упомянутые бренды (не из «похожий на»)
  const brandSet = new Map();
  for (const it of index) if (it.brandN && it.brandN.length >= 3) brandSet.set(it.brandN, it.p.brand);
  const brands = [];
  const tlNoRef = refText ? tl.replace(refText, " ") : tl;
  for (const [bn, name] of brandSet) {
    if (bn.length < 4 && !new RegExp(`(^|\\s)${bn.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\s|$)`).test(tlNoRef)) continue;
    if (` ${tlNoRef} `.includes(` ${bn} `) && !brands.includes(name)) brands.push(name);
  }
  // запрос — просто название аромата без «похож» («Baccarat Rouge 540»): это и есть искомое + похожие
  let exact = null;
  const onlyBrands = brands.length && !scentNorm(tl).split(" ").filter((w) => w.length > 2 && !brands.some((b) => scentNorm(b).includes(w))).length;
  if (!reference && !labels.length && !moodLabels.length && !onlyBrands) {
    const cand = findProduct(tl);
    if (cand && tl.split(" ").length <= 6) { reference = cand; exact = cand; }
  }

  if (!qA.size && !qN.size && !reference && !brands.length && !groups.length) {
    const def = gender === "male" ? { Woody: 0.6, Aromatic: 0.4, Fresh: 0.3, Spicy: 0.2 }
      : gender === "female" ? { Floral: 0.6, Powdery: 0.3, Musky: 0.3, Fruity: 0.2 }
      : { Floral: 0.3, Woody: 0.3, Fresh: 0.3, Amber: 0.2 };
    addW(qA, def);
    moodLabels.push(gift ? "универсальный подарок" : "универсальный");
  }
  if (reference) { const rb = scentNorm(reference.p.brand); for (let i = brands.length - 1; i >= 0; i--) if (rb.includes(scentNorm(brands[i])) || scentNorm(brands[i]).includes(rb)) brands.splice(i, 1); }
  // «дешевле X» — минимум на 30% дешевле обычного флакона X (эталон уже выбран среди 50–100 мл)
  const refCap = reference ? Math.round(reference.p.priceRub * 0.7) : 0;
  return { groups, refCap, raw, text, qA, qN, labels, negLabels, moodLabels, seasons, light, strong, gender, price, sample, wantSet, gift, reference, exact, brands, cheaperThanRef: !!reference && /дешевл|подешевл|бюджетн|недорог/.test(text) };
}

// ── Ранжирование ──────────────────────────────────────────────────────────────
function scentVecSim(a, b) {
  // косинус по аккордам (вес 1) и нотам (вес 1.2)
  let dot = 0, na = 0, nb = 0;
  for (const [k, w] of a.acc) { na += w * w; const v = b.acc.get(k); if (v) dot += w * v; }
  for (const [, w] of b.acc) nb += w * w;
  for (const [k, w] of a.notes) { na += 1.44 * w * w; const v = b.notes.get(k); if (v) dot += 1.44 * w * v; }
  for (const [, w] of b.notes) nb += 1.44 * w * w;
  return na && nb ? dot / Math.sqrt(na * nb) : 0;
}

function scentRank(q, index, groupSets = []) {
  const posMass = [...q.qA.values(), ...q.qN.values()].filter((w) => w > 0).reduce((s, w) => s + w, 0);
  const ref = q.reference;
  let maxPrice = q.price.max || 0;
  if (q.cheaperThanRef && ref) maxPrice = maxPrice ? Math.min(maxPrice, q.refCap) : q.refCap;
  const softMax = q.price.cheap && !maxPrice ? 9000 : 0;

  const scored = [];
  for (const it of index) {
    const p = it.p;
    if (maxPrice && p.priceRub > maxPrice) continue;
    if (q.price.min && p.priceRub < q.price.min) continue;
    if (q.sample ? it.ml > 12 || it.ml === 0 : it.ml > 0 && it.ml < 15) continue; // пробники — только когда просят
    if (it.set && !q.wantSet) continue;
    if (groupSets.length && !groupSets.every((g) => g.has(String(p.offerId).toLowerCase()))) continue;
    if (q.gender === "female" && it.gender === "male") continue;
    if (q.gender === "male" && it.gender === "female") continue;
    if (q.gender === "unisex" && it.gender !== "unisex") continue;
    const isRef = ref && (it.id === ref.id || (ref.refKey?.length && ref.refKey.every((t) => it.norm.includes(t))));
    if (isRef && !q.exact) continue; // сам эталон (любой объём и концентрация) не предлагаем как «похожий»
    if (!it.hasNotes && !ref && posMass === 0 && !q.brands.length && !groupSets.length) continue;

    let score = 0;
    const why = [];
    // совпадение с запросом по аккордам и нотам
    let match = 0;
    const contrib = [];
    for (const [a, w] of q.qA) { const v = it.acc.get(a); if (v) { match += w * v; if (w > 0) contrib.push([a, w * v]); } }
    for (const [n, w] of q.qN) {
      let v = it.notes.get(n) || 0;
      if (!v && w > 0 && scentRu(n) !== n && it.norm.includes(n)) v = 0.6; // нота в названии («Vanilla», «Oud»)
      if (v) { match += 1.15 * w * v; if (w > 0) contrib.push([n, 1.15 * w * v]); }
    }
    if (posMass > 0) score += match / Math.sqrt(posMass);
    // похожесть на эталон
    let sim = 0;
    if (ref && ref.hasNotes && it.hasNotes) {
      sim = scentVecSim(ref, it);
      score += 1.6 * sim;
      const shared = [...ref.notes.keys()].filter((n) => it.notes.has(n))
        .sort((x, y) => (SCENT_RU[y] ? 1 : 0) - (SCENT_RU[x] ? 1 : 0) || (it.notes.get(y) || 0) - (it.notes.get(x) || 0)).slice(0, 3);
      if (shared.length) why.push(`как в ${scentShortName(ref.p)}: ${shared.map(scentRu).join(", ")}`);
    }
    if (q.exact && isRef) score += it.id === ref.id ? 6 : 5;
    if (groupSets.length) { score += 0.3; if (!it.hasNotes) why.push(q.groups.map((g) => `${g} парфюмерия`).join(", ")); } // всё из группы уже подходит
    // бренд
    if (q.brands.length) {
      if (q.brands.some((b) => scentNorm(b) === it.brandN)) { score += posMass || ref ? 0.35 : 1; why.push(`бренд ${p.brand}`); }
      else if (!posMass && !ref) continue;
    }
    // сезон, стойкость, лёгкость
    if (q.seasons.size && [...q.seasons].some((s) => it.seasons.has(s))) score += 0.12;
    if (q.strong) score += it.strong ? 0.18 : it.light ? -0.12 : 0;
    if (q.light) score += it.light ? 0.1 : it.strong ? -0.1 : 0;
    if (q.gender !== "any" && it.gender === q.gender) score += 0.06;
    // бюджет: при «недорого» чуть выше то, что дешевле
    if (softMax) score += p.priceRub <= softMax ? 0.15 * (1 - p.priceRub / softMax) : -0.2;
    // полноценный флакон лучше тестера, у отзывов небольшой вес
    if (it.tester) score -= 0.04;
    score += 0.03 * it.pop;
    if (!it.hasNotes) score *= 0.6;
    if (score <= 0.05 && !(q.exact && isRef)) continue;

    contrib.sort((x, y) => y[1] - x[1]);
    const whyTop = [...new Set(contrib.map(([k]) => scentRu(k)))].slice(0, 3);
    scored.push({ it, score, sim, why: [...whyTop.length ? [whyTop.join(" · ")] : [], ...why] });
  }
  scored.sort((a, b) => b.score - a.score);

  // один аромат — одна карточка; не больше двух от бренда (если бренд не просили)
  const seen = new Set(), perBrand = new Map(), out = [];
  const brandCap = q.brands.length ? 12 : 2;
  for (const s of scored) {
    if (seen.has(s.it.id)) continue;
    const bn = s.it.brandN;
    const refEdition = q.exact && q.reference?.refKey?.every((t) => s.it.norm.includes(t));
    if (!refEdition && (perBrand.get(bn) || 0) >= brandCap) continue;
    seen.add(s.it.id);
    perBrand.set(bn, (perBrand.get(bn) || 0) + 1);
    out.push(s);
    if (out.length >= 12) break;
  }
  const top = out[0]?.score || 1;
  return out.map((s) => ({ ...s, pct: Math.max(55, Math.min(99, Math.round(60 + 39 * (s.score / top)))) }));
}

function scentLabel(q) {
  const parts = [];
  if (q.exact) parts.push(`${scentShortName(q.exact.p)} и похожие ароматы`);
  else if (q.reference) parts.push(`Похоже на ${scentShortName(q.reference.p)}`);
  if (!q.reference && q.brands.length) parts.push(q.brands.slice(0, 2).join(", "));
  if (q.groups.length) parts.push(q.groups.map((g) => `${g} парфюмерия`).join(", "));
  const style = [...q.labels.slice(0, 3), ...q.moodLabels.filter((m) => !m.startsWith("универсальн") || !q.groups.length).slice(0, 2)];
  if (style.length) parts.push(style.join(", "));
  if (q.gender !== "any") parts.push(q.gender === "male" ? "мужской" : q.gender === "female" ? "женский" : "унисекс");
  const s = parts.join(" · ") || "Подборка ароматов";
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function scentUnderstood(q, maxPriceUsed) {
  const chips = [];
  if (q.reference && !q.exact) chips.push({ kind: "ref", text: `похоже на ${scentShortName(q.reference.p)}` });
  if (q.gender !== "any") chips.push({ kind: "gender", text: q.gender === "male" ? "для него" : q.gender === "female" ? "для неё" : "унисекс" });
  q.labels.forEach((l) => chips.push({ kind: "note", text: l }));
  q.moodLabels.forEach((l) => chips.push({ kind: "mood", text: l }));
  q.negLabels.forEach((l) => chips.push({ kind: "neg", text: `без: ${l}` }));
  if (q.brands.length) chips.push({ kind: "brand", text: q.brands.slice(0, 3).join(", ") });
  q.groups.forEach((g) => chips.push({ kind: "brand", text: `${g} парфюмерия` }));
  if (maxPriceUsed) chips.push({ kind: "price", text: `до ${Math.round(maxPriceUsed).toLocaleString("ru-RU")} ₽` });
  if (q.price.min) chips.push({ kind: "price", text: `от ${q.price.min.toLocaleString("ru-RU")} ₽` });
  if (q.price.cheap && !maxPriceUsed) chips.push({ kind: "price", text: "недорого" });
  if (q.strong) chips.push({ kind: "mood", text: "стойкий" });
  if (q.sample) chips.push({ kind: "mood", text: "пробники" });
  return chips;
}

// уточнения одной кнопкой — фраза добавляется к запросу
function scentRefinements(q) {
  const r = [];
  if (!q.price.max && !q.price.cheap && !q.cheaperThanRef) r.push("подешевле");
  if (q.gender === "any") r.push("для неё", "для него");
  if (!q.strong) r.push("стойкий, со шлейфом");
  if (!q.light) r.push("полегче, ненавязчивый");
  if (!q.labels.includes("свежий")) r.push("посвежее");
  if (!q.labels.includes("сладкий") && !q.negLabels.includes("сладкий")) r.push("послаще");
  else if (!q.negLabels.includes("сладкий")) r.push("не сладкий");
  if (!q.sample) r.push("сначала пробники");
  return r.slice(0, 6);
}

async function shopScentSearch(query) {
  const index = await getScentIndex();
  const q = scentParse(query, index);
  const groupSets = (await Promise.all(q.groups.map((g) => shopCollectionOfferIds(g)))).filter(Boolean);
  let results = scentRank(q, index, groupSets);
  // ничего не нашлось с жёсткими фильтрами — ослабляем пол и цену, чтобы не показывать пустоту
  let relaxed = false;
  if (!results.length && (q.gender !== "any" || q.price.max || q.brands.length || q.sample)) {
    if (q.sample) q.sample = false; // пробников такого нет — покажем флаконы
    results = scentRank({ ...q, gender: "any", brands: [], price: { ...q.price, max: q.price.max ? q.price.max * 1.4 : 0 } }, index, groupSets);
    relaxed = results.length > 0;
  }
  const maxPriceUsed = q.cheaperThanRef && q.reference ? Math.min(q.price.max || Infinity, q.refCap) : q.price.max;
  const noteLabels = q.labels;
  return {
    ok: true,
    engine: "scent-v2",
    label: scentLabel(q),
    understood: scentUnderstood(q, Number.isFinite(maxPriceUsed) ? maxPriceUsed : 0),
    refinements: scentRefinements(q),
    relaxed,
    reference: q.reference ? { offerId: q.reference.p.offerId, name: q.reference.p.name, short: scentShortName(q.reference.p), brand: q.reference.p.brand, priceRub: q.reference.p.priceRub, images: q.reference.p.images } : null,
    // старые поля для совместимости с прежним клиентом
    terms: q.brands, notes: noteLabels, accords: q.moodLabels,
    products: results.map(({ it, why, pct }) => ({
      ...it.p,
      _why: why,
      _match: pct,
      _notes: [...it.notes.entries()].sort((a, b) => (SCENT_RU[b[0]] ? 1 : 0) - (SCENT_RU[a[0]] ? 1 : 0) || b[1] - a[1]).slice(0, 5).map(([n]) => scentRu(n)),
    })),
  };
}

// build the index shortly after start so the first visitor doesn't wait ~8 s
if (process.env.SERVER_ROLE !== "worker") setTimeout(() => { getScentIndex().catch((e) => logger.warn("scent index warmup failed", { detail: e?.message })); }, 20000);
