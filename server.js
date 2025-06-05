const express = require("express");
const Queue = require("bull");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(express.json());

const ingestionQueue = new Queue("ingestion");
const statusStore = {};

const PRIORITY_ORDER = { HIGH: 1, MEDIUM: 2, LOW: 3 };

app.post("/ingest", async (req, res) => {
    const { ids, priority } = req.body;

    if (!ids || !priority || !PRIORITY_ORDER[priority]) {
        return res.status(400).json({ error: "Invalid request payload" });
    }

    const ingestionId = uuidv4();
    statusStore[ingestionId] = { ingestion_id: ingestionId, status: "yet_to_start", batches: [] };

    for (let i = 0; i < ids.length; i += 3) {
        const batchIds = ids.slice(i, i + 3);
        const batchId = uuidv4();
        statusStore[ingestionId].batches.push({ batch_id: batchId, ids: batchIds, status: "yet_to_start" });

        await ingestionQueue.add({ batchId, ingestionId, ids: batchIds, priority }, { priority: PRIORITY_ORDER[priority], delay: (i / 3) * 5000 });
    }

    res.json({ ingestion_id: ingestionId });
});

app.get("/status/:ingestion_id", (req, res) => {
    const ingestionId = req.params.ingestion_id;
    const ingestionStatus = statusStore[ingestionId];
    if (!ingestionStatus) {
        return res.status(404).json({ error: "Ingestion ID not found" });
    }

    const allStatuses = ingestionStatus.batches.map(b => b.status);
    ingestionStatus.status =
        allStatuses.every(s => s === "completed") ? "completed" :
        allStatuses.some(s => s === "triggered") ? "triggered" :
        "yet_to_start";

    res.json(ingestionStatus);
});

ingestionQueue.process(async (job) => {
    const { batchId, ingestionId, ids } = job.data;

    statusStore[ingestionId].batches.find(b => b.batch_id === batchId).status = "triggered";

    await new Promise(resolve => setTimeout(resolve, 5000));

    statusStore[ingestionId].batches.find(b => b.batch_id === batchId).status = "completed";
});

app.listen(5000, () => {
    console.log("Server running on http://localhost:5000");
});