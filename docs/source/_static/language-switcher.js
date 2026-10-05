function createLanguageSwitcher() {
    const path = window.location.pathname;
    const currentLanguage =
        (document.documentElement.lang || "en").toLowerCase();

    if (document.querySelector(".language-switcher")) {
        return;
    }

    let englishPath;
    let spanishPath;
    let staticBase;

    if (path.includes("/en/")) {
        englishPath = path;
        spanishPath = path.replace("/en/", "/es/");

        staticBase =
            path.substring(0, path.indexOf("/en/") + 4) +
            "_static/";
    } else if (path.includes("/es/")) {
        spanishPath = path;
        englishPath = path.replace("/es/", "/en/");

        staticBase =
            path.substring(0, path.indexOf("/es/") + 4) +
            "_static/";
    } else {
        englishPath = "/en/";
        spanishPath = "/es/";
        staticBase = "_static/";
    }

    const container = document.createElement("div");
    container.className = "language-switcher";

    const englishButton = document.createElement("button");
    englishButton.type = "button";
    englishButton.className = "language-link";

    englishButton.innerHTML = `
        <img
            src="${staticBase}flags/gb.svg"
            alt="English"
            class="language-flag"
        >
        <span>English</span>
    `;

    const separator = document.createElement("span");
    separator.className = "language-separator";
    separator.textContent = "|";

    const spanishButton = document.createElement("button");
    spanishButton.type = "button";
    spanishButton.className = "language-link";

    spanishButton.innerHTML = `
        <img
            src="${staticBase}flags/es.svg"
            alt="Español"
            class="language-flag"
        >
        <span>Español</span>
    `;

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

createLanguageSwitcher();