import { ProjectsClient } from '@google-cloud/resource-manager';

const projectsClient = new ProjectsClient();

export const getProjectNumber = async () => {
    const projectId = await projectsClient.getProjectId();
    const project = await projectsClient
        .getProject({ name: projectsClient.projectPath(projectId) })
        .then(([response]) => response);
    const [_, projectNumber] = project.name!.split('/');
    return projectNumber;
};
