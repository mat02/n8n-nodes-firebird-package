"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.copyInputItems = void 0;
function copyInputItems(items, properties) {
    let newItem;
    return items.map((item) => {
        newItem = {};
        for (const property of properties) {
            if (item.json[property] === undefined) {
                newItem[property] = null;
            }
            else {
                newItem[property] = JSON.parse(JSON.stringify(item.json[property]));
            }
        }
        return newItem;
    });
}
exports.copyInputItems = copyInputItems;
//# sourceMappingURL=GenericFunctions.js.map