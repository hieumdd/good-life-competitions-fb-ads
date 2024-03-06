import { setTimeout } from 'node:timers/promises';

import { getLogger } from '../logging.service';
import { getClient, getExtractStream } from './api.service';
import { FacebookRequestOptions } from '../pipeline/pipeline.request.dto';

const logger = getLogger(__filename);

export type GetInsightsConfig = {
    level: string;
    fields: string[];
    breakdowns?: string;
};

export const getInsightsStream = (config: GetInsightsConfig) => {
    return async (options: FacebookRequestOptions) => {
        const client = await getClient();

        const requestReport = async (): Promise<string> => {
            const { accountId, start: since, end: until } = options;
            const { level, fields, breakdowns } = config;

            return await client
                .request<{ report_run_id: string }>({
                    method: 'POST',
                    url: `/act_${accountId}/insights`,
                    data: {
                        level,
                        fields,
                        breakdowns,
                        time_range: JSON.stringify({ since, until }),
                        time_increment: 1,
                    },
                })
                .then(({ data }) => data.report_run_id);
        };

        const pollReport = async (reportId: string, delay = 10_000): Promise<string> => {
            const data = await client
                .request<{ async_percent_completion: number; async_status: string }>({
                    method: 'GET',
                    url: `/${reportId}`,
                })
                .then((response) => response.data);

            if (data.async_percent_completion === 100 && data.async_status === 'Job Completed') {
                return reportId;
            }

            if (data.async_status === 'Job Failed') {
                logger.error('Facebook async job failed', data);
                throw new Error(data.async_status);
            }

            if (delay > 5 * 60_000) {
                logger.error('Facebook async job timeout', data);
                throw new Error('Job Timeout');
            }

            await setTimeout(delay * 2);

            return pollReport(reportId);
        };

        return await requestReport()
            .then(pollReport)
            .then((reportId) => {
                return getExtractStream(client, (after) => ({
                    method: 'GET',
                    url: `/${reportId}/insights`,
                    params: { after, limit: 500 },
                }));
            });
    };
};
