#!/usr/bin/env groovy

@Library('lco-shared-libs@0.0.10') _

pipeline {
	agent any
	environment {
		dockerImage = null
		PROJ_NAME = projName()
		GIT_DESCRIPTION = gitDescribe()
		DOCKER_IMG = dockerImageName("${LCO_DOCK_REG}", "${PROJ_NAME}", "${GIT_DESCRIPTION}")
		RANCHERDEV_CREDS = credentials('rancher-cli-dev')
		SSH_CREDS = credentials('jenkins-rancher-ssh-userpass')
	}
	options {
		timeout(time: 8, unit: 'HOURS')
		lock resource: 'BANZAILock'
	}
	stages {
		stage('Build image') {
			steps {
				script {
					dockerImage = docker.build("${DOCKER_IMG}", "--build-arg MINICONDA_VERSION=4.5.11 .")
				}
			}
		}
		stage('Push image') {
			steps {
				script {
					dockerImage.push("${GIT_DESCRIPTION}")
				}
			}
		}
		stage('Unit Tests') {
			steps {
				script {
					sh 'docker run --rm -w=/lco/banzai/ --user=root "${DOCKER_IMG}" python setup.py test -a "-m \'not e2e\'"'
				}
			}
		}
		stage('DeployTestStack') {
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					withCredentials([usernamePassword(
							credentialsId: 'rabbit-mq',
							usernameVariable: 'RABBITMQ_USER',
							passwordVariable: 'RABBITMQ_PASSWORD')]) {
						sh('rancher -c ${RANCHERDEV_CREDS} rm --stop --type stack BANZAITest || true')
						sh('rancher -c ${RANCHERDEV_CREDS} up --stack BANZAITest --force-upgrade --confirm-upgrade -d')
					}
					CONTAINER_ID = getContainerId('BANZAITest-BANZAITest-1')
					CONTAINER_HOST = getContainerHostName('BANZAITest-BANZAITest-1')
				}
			}
		}
		stage('Test-Master-Bias-Creation') {
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					sshagent(credentials: ['jenkins-rancher-ssh']) {
						executeOnRancher('banzai_run_end_to_end_tests --marker=master_bias ' +
						    '--junit-file=/archive/engineering/pytest-master-bias.xml --code-path=/lco/banzai',
						    CONTAINER_HOST, CONTAINER_ID, 'root:root')
					}
				}
			}
			post {
				always {
					script{
						sshagent(credentials: ['jenkins-rancher-ssh']) {
							copyFromRancherContainer('/archive/engineering/pytest-master-bias.xml', '.', CONTAINER_HOST, CONTAINER_ID)
						}
						junit 'pytest-master-bias.xml'
					}
				}
			}
		}
		stage('Test-Master-Dark-Creation') {
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					sshagent(credentials: ['jenkins-rancher-ssh']) {
						executeOnRancher('banzai_run_end_to_end_tests --marker=master_dark ' +
						    '--junit-file=/archive/engineering/pytest-master-dark.xml --code-path=/lco/banzai',
							CONTAINER_HOST, CONTAINER_ID, 'root:root')
					}
				}
			}
			post {
				always {
					script{
						sshagent(credentials: ['jenkins-rancher-ssh']) {
							copyFromRancherContainer('/archive/engineering/pytest-master-dark.xml', '.', CONTAINER_HOST, CONTAINER_ID)
						}
						junit 'pytest-master-dark.xml'
					}
				}
			}
		}
		stage('Test-Master-Flat-Creation') {
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					sshagent(credentials: ['jenkins-rancher-ssh']) {
						executeOnRancher('banzai_run_end_to_end_tests --marker=master_flat ' +
						    '--junit-file=/archive/engineering/pytest-master-flat.xml --code-path=/lco/banzai',
							CONTAINER_HOST, CONTAINER_ID, 'root:root')
					}
				}
			}
			post {
				always {
					script{
						sshagent(credentials: ['jenkins-rancher-ssh']) {
							copyFromRancherContainer('/archive/engineering/pytest-master-flat.xml', '.', CONTAINER_HOST, CONTAINER_ID)
						}
						junit 'pytest-master-flat.xml'
					}
				}
			}
		}
		stage('Test-Science-File-Creation') {
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					sshagent(credentials: ['jenkins-rancher-ssh']) {
						executeOnRancher('banzai_run_end_to_end_tests --marker=science_files ' +
						    '--junit-file=/archive/engineering/pytest-science-files.xml --code-path=/lco/banzai',
							CONTAINER_HOST, CONTAINER_ID, 'root:root')
					}
				}
			}
			post {
				always {
					script{
						sshagent(credentials: ['jenkins-rancher-ssh']) {
							copyFromRancherContainer('/archive/engineering/pytest-science-files.xml', '.', CONTAINER_HOST, CONTAINER_ID)
						}
						junit 'pytest-science-files.xml'
					}
				}
				success {
					script {
						sh('rancher -c ${RANCHERDEV_CREDS} rm --stop --type stack BANZAITest')
					}
				}
			}
		}
	}
}

