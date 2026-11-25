//! Idioma do plugin.
//!
//! O texto do manifesto e das views (títulos, botões, rótulos de campos,
//! colunas, nós da árvore, status) é escrito em **inglês** e traduzido pelo IDE
//! a partir de `locales/pt-BR.json`, que mapeia texto-fonte → tradução.
//!
//! O texto que o plugin constrói (notificações, prompts, mensagens de erro,
//! painéis de detalhes e a página HTML) não passa por essa tradução: usamos o
//! mesmo catálogo, embutido com `include_str!`, e `host::locale()` para decidir
//! o idioma. Assim há uma única fonte de verdade e o padrão é `en-US`.

use openidea_plugin::host;
use std::collections::BTreeMap;
use std::sync::OnceLock;

const PT_BR: &str = include_str!("../locales/pt-BR.json");

fn catalog() -> &'static BTreeMap<String, String> {
    static CATALOG: OnceLock<BTreeMap<String, String>> = OnceLock::new();
    CATALOG.get_or_init(|| serde_json::from_str(PT_BR).unwrap_or_default())
}

/// O catálogo completo, para a página HTML aplicar as mesmas traduções.
pub fn catalog_json() -> &'static str {
    PT_BR
}

/// `auto` (padrão) segue a linguagem da IDE; o usuário pode fixar o idioma.
pub fn portuguese() -> bool {
    match host::setting_string("language").as_deref() {
        Some("pt-BR") | Some("pt") => return true,
        Some("en-US") | Some("en") => return false,
        _ => {}
    }
    host::locale().to_ascii_lowercase().starts_with("pt")
}

/// Traduz um texto simples. Sem entrada no catálogo, mantém o inglês.
pub fn t(en: &str) -> String {
    if portuguese() {
        catalog().get(en).cloned().unwrap_or_else(|| en.into())
    } else {
        en.into()
    }
}

/// Traduz um texto com espaços reservados `{nome}`.
pub fn tf(en: &str, args: &[(&str, &str)]) -> String {
    let mut out = t(en);
    for (key, value) in args {
        out = out.replace(&format!("{{{key}}}"), value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn english_is_the_default() {
        let _host = crate::mock_host(); // locale "en"
        assert_eq!(t("Refresh"), "Refresh");
        assert_eq!(tf("Topic {name} created.", &[("name", "pedidos")]), "Topic pedidos created.");
    }

    #[test]
    fn follows_the_ide_locale() {
        let host = crate::mock_host();
        host.set_locale("pt-BR");
        assert_eq!(t("Refresh"), "Atualizar");
        assert_eq!(tf("Topic {name} created.", &[("name", "pedidos")]), "Tópico pedidos criado.");
    }

    #[test]
    fn a_fixed_language_overrides_the_ide() {
        let host = crate::mock_host();
        host.set_locale("en");
        host.set_setting("language", r#""pt-BR""#);
        assert_eq!(t("Refresh"), "Atualizar");
        host.set_setting("language", r#""en-US""#);
        assert_eq!(t("Refresh"), "Refresh");
    }

    #[test]
    fn unknown_text_falls_back_to_english() {
        let host = crate::mock_host();
        host.set_locale("pt-BR");
        assert_eq!(t("Something not in the catalog"), "Something not in the catalog");
    }
}
