import { CloudTasksClient, protos } from '@google-cloud/tasks';
import HttpMethod = protos.google.cloud.tasks.v2.HttpMethod;
import { v4 as uuidv4 } from 'uuid';

const tasksClient = new CloudTasksClient();

export type QueueConfig = { queue: string; location: string };

export const createTasks = async <P>({ location, queue }: QueueConfig, payloads: P[], nameFn: (p: P) => string) => {
    const URL = process.env.PUBLIC_URL || '';

    const [projectId, serviceAccountEmail] = await Promise.all([
        tasksClient.getProjectId(),
        tasksClient.auth.getCredentials().then((credentials) => credentials.client_email),
    ]);

    const tasks = payloads.map((p) => ({
        parent: tasksClient.queuePath(projectId, location, queue),
        task: {
            name: tasksClient.taskPath(projectId, location, queue, `${nameFn(p)}-${uuidv4()}`),
            httpRequest: {
                httpMethod: HttpMethod.POST,
                headers: { 'Content-Type': 'application/json' },
                url: URL,
                oidcToken: { serviceAccountEmail },
                body: Buffer.from(JSON.stringify(p)).toString('base64'),
            },
        },
    }));

    return await Promise.all(tasks.map((r) => tasksClient.createTask(r))).then(() => tasks.length);
};
