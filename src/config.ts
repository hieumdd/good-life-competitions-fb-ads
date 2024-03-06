import { getProjectNumber } from './google-cloud/resource-manager.service';

export const getBucketName = async () => {
    const projectNumber = await getProjectNumber();
    return `facebook-${projectNumber}`;
};

export const DATASET = 'Facebook';

export const QUEUE_CONFIG = {
    location: 'us-central1',
    queue: 'fb-ads',
};
