module.exports = {
    uiPort: process.env.PORT || 1880,
    flowFile: "flows.json",
    credentialSecret: "smart-mobility-assignment-secret",
    functionGlobalContext: {
        proj4: require("proj4"),
    },
    diagnostics: {
        enabled: true,
        ui: true,
    },
    editorTheme: {
        projects: {
            enabled: false,
        },
    },
};
