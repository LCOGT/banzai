#!/usr/bin/env groovy

@Library('lco-shared-libs@0.0.1') _

pipeline {
	agent any
	environment {
		dockerImage = null
		PROJ_NAME = projName("${JOB_NAME}")
		GIT_DESCRIPTION = gitDescription()
		DOCKER_IMG = dockerImageName("${LCO_DOCK_REG}", "${PROJ_NAME}", "${GIT_DESCRIPTION}")
	}
	options {
		timeout(time: 1, unit: 'HOURS')
	}
	stages {
		stage('Build image') {
			steps {
				script {
					dockerImage = docker.build("${DOCKER_IMG}", "--build-arg MINICONDA_VERSION=4.4.10 .")
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
					sh 'docker run --rm -w=/lco/banzai/ --user=root "${DOCKER_IMG}" python setup.py test'
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
						executeOnRancher('pytest --durations=0 --junitxml=/archive/engineering/pytest-master-bias.xml ' +
								'-m master_bias /lco/banzai',
								CONTAINER_HOST, CONTAINER_ID, ARCHIVE_UID)
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
						executeOnRancher('pytest --durations=0 --junitxml=/archive/engineering/pytest-master-dark.xml ' +
								'-m master_dark /lco/banzai',
								CONTAINER_HOST, CONTAINER_ID, ARCHIVE_UID)
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
						executeOnRancher('pytest --durations=0 --junitxml=/archive/engineering/pytest-master-flat.xml ' +
								'-m master_flat /lco/banzai/',
								CONTAINER_HOST, CONTAINER_ID, ARCHIVE_UID)
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
						executeOnRancher('pytest --durations=0 --junitxml=/archive/engineering/pytest-science-files.xml ' +
								'-m science_files /lco/banzai/',
								CONTAINER_HOST, CONTAINER_ID, ARCHIVE_UID)
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

