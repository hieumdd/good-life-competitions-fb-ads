import * as pipelines from './pipeline.const';
import { createInsightsPipelineTasks, runInsightsPipeline } from './pipeline.service';

describe('runInsightsPipeline', () => {
    it.each([pipelines.AdsInsights])(
        'runInsightsPipeline/$name',
        async (pipeline) => {
            return await runInsightsPipeline(pipeline, {
                accountId: '567146027085459',
                start: '2024-03-01',
                end: '2024-04-01',
            })
                .then((results) => expect(results).toBeDefined())
                .catch((error) => {
                    console.error({ error });
                    throw error;
                });
        },
        100_000_000,
    );
});

it('createInsightsPipelineTasks', async () => {
    return await createInsightsPipelineTasks({
        start: '2023-08-28',
        end: '2023-09-04',
    })
        .then((result) => expect(result).toBeDefined())
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
