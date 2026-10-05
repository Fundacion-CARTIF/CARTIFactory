function createLanguageSwitcher() {
    const path = window.location.pathname;
    const currentLanguage =
        (document.documentElement.lang || "en").toLowerCase();

    // Prevent duplicate switchers.
    if (document.querySelector(".language-switcher")) {
        return;
    }

    let englishPath;
    let spanishPath;

    if (path.includes("/en/")) {
        englishPath = path;
        spanishPath = path.replace("/en/", "/es/");
    } else if (path.includes("/es/")) {
        spanishPath = path;
        englishPath = path.replace("/es/", "/en/");
    } else {
        englishPath = "/en/";
        spanishPath = "/es/";
    }

    const container = document.createElement("div");
    container.className = "language-switcher";

    const englishButton = document.createElement("button");
    englishButton.type = "button";
    englishButton.className = "language-link";
    englishButton.textContent = "🇬🇧 English";

    const separator = document.createElement("span");
    separator.className = "language-separator";
    separator.textContent = "|";

    const spanishButton = document.createElement("button");
    spanishButton.type = "button";
    spanishButton.className = "language-link";
    spanishButton.textContent = "🇪🇸 Español";

    if (currentLanguage.startsWith("es")) {
        spanishButton.classList.add("active");
    } else {
        englishButton.classList.add("active");
    }

    englishButton.addEventListener("click", function () {
        window.location.assign(englishPath);
    });

    spanishButton.addEventListener("click", function () {
        window.location.assign(spanishPath);
    });

    container.appendChild(englishButton);
    container.appendChild(separator);
    container.appendChild(spanishButton);

    document.body.appendChild(container);
}

if (document.readyState === "loading") {
    document.addEventListener(
        "DOMContentLoaded",
        createLanguageSwitcher
    );
} else {
    createLanguageSwitcher();
}