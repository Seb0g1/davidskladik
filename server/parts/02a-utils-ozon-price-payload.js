function buildOzonPricePayload(item = {}) {
  const price = roundPrice(item.price);
  const payload = {
    offer_id: String(item.offerId || item.offer_id || "").trim(),
    price: String(price),
    currency_code: "RUB",
  };
  const forceOldPrice = item.forceOldPrice === true || item.retryReason === "ozon_old_price_adjusted";
  if (forceOldPrice || parseBooleanSetting(process.env.OZON_PRICE_PUSH_SET_OLD_PRICE, true)) {
    const oldPrice = resolveOzonOldPrice(price, item);
    payload.old_price = String(oldPrice);
  } else if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_RESET_OLD_PRICE, false)) {
    payload.old_price = "0";
  }
  if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS, true)) {
    payload.auto_action_enabled = "DISABLED";
    payload.price_strategy_enabled = "DISABLED";
  }
  if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_SET_MIN_PRICE, false)) {
    payload.min_price = String(price);
  }
  return payload;
}

function resolveOzonOldPrice(price, item = {}) {
  const currentPrice = roundPrice(price);
  if (!currentPrice) return 0;
  const markupPct = Math.max(0, Number(process.env.OZON_OLD_PRICE_MARKUP_PCT || 20) || 20);
  const lowPriceMax = Math.max(100, Number(process.env.OZON_LOW_PRICE_MAX_RUB || 400) || 400);
  const lowPriceMinDiscountPct = Math.max(20, Number(process.env.OZON_LOW_PRICE_MIN_DISCOUNT_PCT || 21) || 21);
  const markupOldPrice = roundPrice(currentPrice * (1 + markupPct / 100));
  const lowPriceMinOld = currentPrice <= lowPriceMax
    ? Math.ceil(currentPrice / (1 - lowPriceMinDiscountPct / 100))
    : 0;
  const requestedOldPrice = roundPrice(item.oldPrice ?? item.old_price ?? item.oldPriceRub ?? 0);
  return Math.max(currentPrice + 1, markupOldPrice, lowPriceMinOld, requestedOldPrice);
}

function getOzonMaxPriceDropRatioPerStep() {
  return Math.max(0.105, Math.min(0.25, Number(process.env.OZON_MAX_PRICE_DROP_RATIO_PER_STEP || 0.11) || 0.11));
}

function computeOzonQuarantineNextPrice(cabinetPrice, targetPrice) {
  const cabinet = roundPrice(cabinetPrice);
  const target = roundPrice(targetPrice);
  if (!cabinet || !target) return target;
  if (target >= cabinet) return target;
  const minNext = Math.ceil(cabinet * getOzonMaxPriceDropRatioPerStep());
  return minNext <= target ? target : minNext;
}

function planOzonQuarantinePriceSteps(cabinetPrice, targetPrice, { maxSteps = 24 } = {}) {
  const target = roundPrice(targetPrice);
  let current = roundPrice(cabinetPrice);
  if (!target) return [];
  if (!current || target >= current) return [target];
  const steps = [];
  while (steps.length < maxSteps && current > target) {
    const next = computeOzonQuarantineNextPrice(current, target);
    if (!next || next >= current) break;
    steps.push(next);
    current = next;
  }
  if (!steps.length || steps[steps.length - 1] !== target) {
    if (computeOzonQuarantineNextPrice(current, target) === target) steps.push(target);
  }
  return steps;
}


