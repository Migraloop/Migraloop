// Lab DB-level load escape hatch (issue #87).
// Load into disposable Lab Mongo via compose exec mongosh — not a Lab Scenario.
// Collection is intentionally outside Delivery Managed Columns so operators can
// inspect raw Target-side seed data without colliding with apply Initial Load.
db.lab_escape_manual.deleteMany({});
db.lab_escape_manual.insertMany([
  { _id: 1, note: "escape-hatch seed", source: "mongo-load.js" },
  { _id: 2, note: "inspect with mongosh or product target when bound", source: "mongo-load.js" },
]);
print("lab_escape_manual count=" + db.lab_escape_manual.countDocuments({}));
