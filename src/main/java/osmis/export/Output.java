package osmis.export;

public class Output {
    public boolean completedSuccessfully;
    public String message;
    public String tableName;
    public Long runDuration;
    public Long batchSize;
    public Long batchCount;
    public Long recordsExported;

    public Output(boolean completedSuccessfully, long batchSize, long batchCount, long recordsExported, long runDuration, String tableName, String message) {
        this.completedSuccessfully = completedSuccessfully;
        this.tableName = tableName;
        this.batchCount = batchCount;
        this.batchSize = batchSize;
        this.recordsExported = recordsExported;
        this.runDuration = runDuration;

        if (message.trim().isBlank()) {
            this.message = String.format("Exported %d records to %s, in %d batches of size %d. Time to export: %d milliseconds. " + (completedSuccessfully ? "Export completed successfully." : "Export failed to complete."),recordsExported, tableName, batchCount, batchSize, runDuration);
        }else {
            this.message = message;
        }
    }
}