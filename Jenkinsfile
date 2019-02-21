#!/usr/bin/env groovy

@Library('lco-shared-libs@0.0.10') _

pipeline {
	agent any
	environment {
		dockerImage = null
		PROJ_NAME = projName()
		GIT_DESCRIPTION = gitDescribe()
		DOCKER_IMG = dockerImageName("${LCO_DOCK_REG}", "${PROJ_NAME}", "${GIT_DESCRIPTION}")
		KUBERNETES_CREDS = credentials('jenkins-kubeconfig')
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
					sh 'docker run --rm -w=/lco/banzai/ "${DOCKER_IMG}" python setup.py test -a "-m \'not e2e\'"'
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
					// delete previous run if the previous failed somehow
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev delete pod banzai-e2e-test || true')
					// we will be testing the image that we just built
					sh('sed -i -e "s^@BANZAI_IMAGE@^${DOCKER_IMG}^g" banzai/tests/e2e-k8s.yaml')
					// deploy the test pod to the cluster
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev apply -f banzai/tests/e2e-k8s.yaml')
					// wait for the test pod to be running
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev wait --for=condition=Ready --timeout=60m pod/banzai-e2e-test')
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
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev exec banzai-e2e-test/BANZAITestRunner -- banzai_run_end_to_end_tests --marker=master_bias --junit-file=/archive/engineering/pytest-master-bias.xml --code-path=/lco/banzai')
				}
			}
			post {
				always {
					script {
						sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev cp banzai-e2e-test:/archive/engineering/pytest-master-bias.xml .')
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
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev exec banzai-e2e-test/BANZAITestRunner -- banzai_run_end_to_end_tests --marker=master_dark --junit-file=/archive/engineering/pytest-master-dark.xml --code-path=/lco/banzai')
				}
			}
			post {
				always {
					script {
						sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev cp banzai-e2e-test:/archive/engineering/pytest-master-dark.xml .')
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
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev exec banzai-e2e-test/BANZAITestRunner -- banzai_run_end_to_end_tests --marker=master_flat --junit-file=/archive/engineering/pytest-master-flat.xml --code-path=/lco/banzai')
				}
			}
			post {
				always {
					script {
						sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev cp banzai-e2e-test:/archive/engineering/pytest-master-flat.xml .')
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
					sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev exec banzai-e2e-test/BANZAITestRunner -- banzai_run_end_to_end_tests --marker=science_files --junit-file=/archive/engineering/pytest-science-files.xml --code-path=/lco/banzai')
				}
			}
			post {
				always {
					script {
						sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev cp banzai-e2e-test:/archive/engineering/pytest-science-files.xml .')
						junit 'pytest-science-files.xml'
					}
				}
				success {
					script {
						sh('kubectl --kubeconfig=${KUBERNETES_CREDS} -n dev delete pod banzai-e2e-test || true')
					}
				}
			}
		}
	}
}

