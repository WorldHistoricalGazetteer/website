const LANG_KEY = 'whg_lang';

function getPreferredLanguage() {
    // 1. Explicit per-visit override via ?lang= takes precedence.
    const urlParams = new URLSearchParams(window.location.search);
    const langParam = urlParams.get('lang');
    if (langParam && languages[langParam]) {
        console.info(`Overriding language to ${langParam} from URL parameter`);
        return langParam;
    }
    // 2. Persisted user preference (set via the map language selector) — shared
    //    site-wide and remembered between visits.
    try {
        const stored = localStorage.getItem(LANG_KEY);
        if (stored && languages[stored]) return stored;
    } catch (e) { /* localStorage unavailable (e.g. private mode) */ }
    // 3. Otherwise fall back to the browser's language.
    const browserLang = navigator.language || navigator.userLanguage;
    const langCode = browserLang.split('-')[0];
    console.info(`Detected browser language: ${browserLang}, using code: ${langCode}`);
    return languages[langCode] ? langCode : 'local'; // Default to 'local' if not found
}

/** Persist (or clear, when given a falsy/unknown code) the user's preferred
 *  language site-wide. Used by the map language selector; read back by
 *  getPreferredLanguage() so every consumer (map labels, Wikipedia links, …)
 *  stays in sync. */
function setPreferredLanguage(code) {
    try {
        if (code && languages[code]) localStorage.setItem(LANG_KEY, code);
        else localStorage.removeItem(LANG_KEY);
    } catch (e) { /* localStorage unavailable */ }
    return code;
}

const languages = {
  "local": {
    "en": "Local",
    "local": "Local"
  },
  "en": {
    "en": "English",
    "local": "English"
  },
  "de": {
    "en": "German",
    "local": "Deutsch"
  },
  "fr": {
    "en": "French",
    "local": "Français"
  },
  "es": {
    "en": "Spanish",
    "local": "Español"
  },
  "cs": {
    "en": "Czech",
    "local": "Čeština"
  },
  "sq": {
    "en": "Albanian",
    "local": "Shqip"
  },
  "ar": {
    "en": "Arabic",
    "local": "العربية"
  },
  "hy": {
    "en": "Armenian",
    "local": "Հայերեն"
  },
  "az": {
    "en": "Azerbaijani",
    "local": "Azərbaycanca"
  },
  "be": {
    "en": "Belarusian",
    "local": "Беларуская"
  },
  "bs": {
    "en": "Bosnian",
    "local": "Bosanski"
  },
  "br": {
    "en": "Breton",
    "local": "Brezhoneg"
  },
  "bg": {
    "en": "Bulgarian",
    "local": "Български"
  },
  "ca": {
    "en": "Catalan",
    "local": "Català"
  },
  "zh": {
    "en": "Chinese",
    "local": "中文"
  },
  "hr": {
    "en": "Croatian",
    "local": "Hrvatski"
  },
  "da": {
    "en": "Danish",
    "local": "Dansk"
  },
  "nl": {
    "en": "Dutch",
    "local": "Nederlands"
  },
  "eo": {
    "en": "Esperanto",
    "local": "Esperanto"
  },
  "et": {
    "en": "Estonian",
    "local": "Eesti"
  },
  "fi": {
    "en": "Finnish",
    "local": "Suomi"
  },
  "ka": {
    "en": "Georgian",
    "local": "ქართული"
  },
  "el": {
    "en": "Greek",
    "local": "Ελληνικά"
  },
  "he": {
    "en": "Hebrew",
    "local": "עברית"
  },
  "hu": {
    "en": "Hungarian",
    "local": "Magyar"
  },
  "is": {
    "en": "Icelandic",
    "local": "Íslenska"
  },
  "ga": {
    "en": "Irish",
    "local": "Gaeilge"
  },
  "it": {
    "en": "Italian",
    "local": "Italiano"
  },
  "ja": {
    "en": "Japanese",
    "local": "日本語"
  },
  "ja_kana": {
    "en": "Japanese (Kana)",
    "local": "日本語 (かな)"
  },
  "ja_rm": {
    "en": "Japanese (Latin)",
    "local": "日本語 (ローマ字)"
  },
  "kn": {
    "en": "Kannada",
    "local": "ಕನ್ನಡ"
  },
  "kk": {
    "en": "Kazakh",
    "local": "Қазақша"
  },
  "ko": {
    "en": "Korean",
    "local": "한국어"
  },
  "ko_rm": {
    "en": "Korean (Latin)",
    "local": "한국어 (로마자)"
  },
  "la": {
    "en": "Latin",
    "local": "Latina"
  },
  "lv": {
    "en": "Latvian",
    "local": "Latviešu"
  },
  "lt": {
    "en": "Lithuanian",
    "local": "Lietuvių"
  },
  "lb": {
    "en": "Luxembourgish",
    "local": "Lëtzebuergesch"
  },
  "mk": {
    "en": "Macedonian",
    "local": "Македонски"
  },
  "mt": {
    "en": "Maltese",
    "local": "Malti"
  },
  "no": {
    "en": "Norwegian",
    "local": "Norsk"
  },
  "pl": {
    "en": "Polish",
    "local": "Polski"
  },
  "pt": {
    "en": "Portuguese",
    "local": "Português"
  },
  "ro": {
    "en": "Romanian",
    "local": "Română"
  },
  "rm": {
    "en": "Romansh",
    "local": "Rumantsch"
  },
  "ru": {
    "en": "Russian",
    "local": "Русский"
  },
  "gd": {
    "en": "Scottish Gaelic",
    "local": "Gàidhlig"
  },
  "sr": {
    "en": "Serbian (Cyrillic)",
    "local": "Српски (ћирилица)"
  },
  "sr-Latn": {
    "en": "Serbian (Latin)",
    "local": "Srpski (latinica)"
  },
  "sk": {
    "en": "Slovak",
    "local": "Slovenčina"
  },
  "sl": {
    "en": "Slovene",
    "local": "Slovenščina"
  },
  "sv": {
    "en": "Swedish",
    "local": "Svenska"
  },
  "th": {
    "en": "Thai",
    "local": "ไทย"
  },
  "tr": {
    "en": "Turkish",
    "local": "Türkçe"
  },
  "uk": {
    "en": "Ukrainian",
    "local": "Українська"
  },
  "cy": {
  	"en": "Welsh",
  	"local": "Cymraeg"
  },
  "fy": {
    "en": "Western Frisian",
    "local": "Frysk"
  }
}

export default languages;
export { getPreferredLanguage, setPreferredLanguage };
