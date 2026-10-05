document.addEventListener("DOMContentLoaded", function () {
    const path = window.location.pathname;

    const isSpanish = path.includes("/es/");
    const currentLang = isSpanish ? "es" : "en";

    let englishPath = path;
    let spanishPath = path;

    if (path.includes("/en/")) {
        spanishPath = path.replace("/en/", "/es/");
    } else if (path.includes("/es/")) {
        englishPath = path.replace("/es/", "/en/");
    } else {
        return;
    }

    const container = document.createElement("div");
    container.className = "language-switcher";

    const englishLink = document.createElement("a");
    englishLink.href = englishPath;
    englishLink.innerHTML = "🇬🇧 English";

    const separator = document.createElement("span");
    separator.textContent = "|";

    const spanishLink = document.createElement("a");
    spanishLink.href = spanishPath;
    spanishLink.innerHTML = "🇪🇸 Español";

    if (currentLang === "en") {
        englishLink.classList.add("active");
    } else {
        spanishLink.classList.add("active");
    }

    container.appendChild(englishLink);
    container.appendChild(separator);
    container.appendChild(spanishLink);

    const content = document.querySelector(".wy-nav-content");

    if (content) {
        content.prepend(container);
    }
});