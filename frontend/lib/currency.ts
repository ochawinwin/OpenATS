/** ISO 4217 code used wherever a currency is not chosen yet (new jobs, new offers, samples). */
export const DEFAULT_CURRENCY = "THB";

/** Currencies offered in the UI. THB comes first; the rest stay so existing records still render. */
export const CURRENCIES = ["THB", "USD", "EUR", "GBP", "LKR", "INR", "AUD"] as const;
