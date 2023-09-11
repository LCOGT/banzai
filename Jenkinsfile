#!/usr/bin/env groovy

@Library('lco-shared-libs@0.0.10') _

pipeline {
	agent any
	parameters {
		booleanParam(
			name: 'forceEndToEnd',
			defaultValue: false,
			description: 'When true, forces the end-to-end tests to always run.')
	}
	environment {
		dockerImage = null
		PROJ_NAME = projName()
		GIT_DESCRIPTION = gitDescribe()
		DOCKER_IMG = dockerImageName("${LCO_DOCK_REG}", "${PROJ_NAME}", "${GIT_DESCRIPTION}")
	}
	options {
		timeout(time: 8, unit: 'HOURS')
		lock resource: 'BANZAILock'
	}
	stages {
		stage('Build image') {
			steps {
				script {
					dockerImage = docker.build("${DOCKER_IMG}", ".")
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
					sh 'echo sup!'
					sh 'docker run --rm "${DOCKER_IMG}" pytest --pyargs banzai.tests -m \'not e2e\''
				}
			}
		}
		stage('DeployProdStack') {
			agent {
				label 'helm'
			}
	        when {
                buildingTag();
	        }
		    steps {
	            script {
                    withKubeConfig([credentialsId: "prod-kube-config"]) {
                        sh('helm repo update && helm dependency update helm-chart/banzai/ '+
                                '&& helm package helm-chart/banzai --app-version="${GIT_DESCRIPTION}" --version="${GIT_DESCRIPTION}" ' +
                                '&& helm upgrade --install banzai banzai-"${GIT_DESCRIPTION}".tgz --namespace=prod ' +
                                '--set image.tag="${GIT_DESCRIPTION}" --values=helm-chart/banzai/values-prod.yaml ' +
                                '--force --atomic --timeout=3600s')
                    }
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
					withKubeConfig([credentialsId: 'build-kube-config']) {
						// delete previous run if the previous failed somehow
						sh('kubectl -n build delete pod banzai-e2e-test || true')
						// we will be testing the image that we just built
						sh('sed -i -e "s^@BANZAI_IMAGE@^${DOCKER_IMG}^g" banzai/tests/e2e-k8s.yaml')
						// deploy the test pod to the cluster
						sh('kubectl -n build apply -f banzai/tests/e2e-k8s.yaml')
						// wait for the test pod to be running
						sh('kubectl -n build wait --for=condition=Ready --timeout=60m pod/banzai-e2e-test')
					}
				}
			}
		}
		stage('Test-Master-Bias-Creation') {
			environment {
				// store stage start time in the environment so it has stage scope
				START_TIME = sh(script: 'date +%s', returnStdout: true).trim()
			}
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					withKubeConfig([credentialsId: 'build-kube-config']) {
						sh('kubectl -n build exec banzai-e2e-test -c banzai-listener -- pytest -s --pyargs banzai.tests --durations=0 --junitxml=/archive/engineering/pytest-master-bias.xml -m master_bias')
					}
				}
			}
			post {
				always {
					script {
						withKubeConfig([credentialsId: 'build-kube-config']) {
							env.LOGS_SINCE = sh(script: 'expr `date +%s` - ${START_TIME}', returnStdout: true).trim()
							sh('kubectl -n build logs --since=${LOGS_SINCE}s --all-containers banzai-e2e-test')
							sh('kubectl -n build cp -c banzai-listener banzai-e2e-test:/archive/engineering/pytest-master-bias.xml pytest-master-bias.xml')
							junit 'pytest-master-bias.xml'
						}
					}
				}
			}
		}
		stage('Test-Master-Dark-Creation') {
			environment {
				// store stage start time in the environment so it has stage scope
				START_TIME = sh(script: 'date +%s', returnStdout: true).trim()
			}
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					withKubeConfig([credentialsId: 'build-kube-config']) {
						sh('kubectl -n build exec banzai-e2e-test -c banzai-listener -- pytest -s --pyargs banzai.tests --durations=0 --junitxml=/archive/engineering/pytest-master-dark.xml -m master_dark')
					}
				}
			}
			post {
				always {
					script {
						withKubeConfig([credentialsId: 'build-kube-config']) {
							env.LOGS_SINCE = sh(script: 'expr `date +%s` - ${START_TIME}', returnStdout: true).trim()
							sh('kubectl -n build logs --since=${LOGS_SINCE}s --all-containers banzai-e2e-test')
							sh('kubectl -n build cp -c banzai-listener banzai-e2e-test:/archive/engineering/pytest-master-dark.xml pytest-master-dark.xml')
							junit 'pytest-master-dark.xml'
						}
					}
				}
			}
		}
		stage('Test-Master-Flat-Creation') {
			environment {
				// store stage start time in the environment so it has stage scope
				START_TIME = sh(script: 'date +%s', returnStdout: true).trim()
			}
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					withKubeConfig([credentialsId: 'build-kube-config']) {
						sh('kubectl -n build exec banzai-e2e-test -c banzai-listener -- pytest -s --pyargs banzai.tests --durations=0 --junitxml=/archive/engineering/pytest-master-flat.xml -m master_flat')
					}
				}
			}
			post {
				always {
					script {
						withKubeConfig([credentialsId: 'build-kube-config']) {
							env.LOGS_SINCE = sh(script: 'expr `date +%s` - ${START_TIME}', returnStdout: true).trim()
							sh('kubectl -n build logs --since=${LOGS_SINCE}s --all-containers banzai-e2e-test')
							sh('kubectl -n build cp -c banzai-listener banzai-e2e-test:/archive/engineering/pytest-master-flat.xml pytest-master-flat.xml')
							junit 'pytest-master-flat.xml'
						}
					}
				}
			}
		}
		stage('Test-Science-File-Creation') {
			environment {
				// store stage start time in the environment so it has stage scope
				START_TIME = sh(script: 'date +%s', returnStdout: true).trim()
			}
			when {
				anyOf {
					branch 'PR-*'
					expression { return params.forceEndToEnd }
				}
			}
			steps {
				script {
					withKubeConfig([credentialsId: 'build-kube-config']) {
						sh('kubectl -n build exec banzai-e2e-test -c banzai-listener -- pytest -s --pyargs banzai.tests --durations=0 --junitxml=/archive/engineering/pytest-science-files.xml -m science_files')
					}
				}
			}
			post {
				always {
					script {
						withKubeConfig([credentialsId: 'build-kube-config']) {
							env.LOGS_SINCE = sh(script: 'expr `date +%s` - ${START_TIME}', returnStdout: true).trim()
							sh('kubectl -n build logs --since=${LOGS_SINCE}s --all-containers banzai-e2e-test')
							sh('kubectl -n build cp -c banzai-listener banzai-e2e-test:/archive/engineering/pytest-science-files.xml pytest-science-files.xml ')
							junit 'pytest-science-files.xml'
						}
					}
				}
				success {
					script {
						withKubeConfig([credentialsId: 'build-kube-config']) {
							sh('kubectl -n build delete pod banzai-e2e-test || true')
						}
					}
				}
			}
		}
	}
}

