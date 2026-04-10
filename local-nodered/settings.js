const path = require("path");

module.exports = {
    uiPort: process.env.PORT || 1880,
    flowFile: path.resolve(__dirname, "../Smart_Mobility_Dynamic_Parking_Flow.json"),
    userDir: path.resolve(__dirname, "runtime"),
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
