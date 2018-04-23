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
	stages {
		stage('Build image') {
			steps {
				script {
					dockerImage = docker.build("${DOCKER_IMG}")
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
		stage('Test') {
		    steps {
		        script {
                    sh 'docker run --rm -w=/lco/banzai/ --user=root "${DOCKER_IMG}" python setup.py test'
		        }
		    }
		}
		stage('Deploy') {
			parallel {
				stage('Dev') {
					when { branch 'dev' }
					environment {
						DEV_CREDS = credentials('rancher-cli-dev')
					}
					steps {
						sh '''
							export DOCKER_IMG="${DOCKER_IMG}"
							rancher -c ${DEV_CREDS} up --stack BANZAI --force-upgrade --confirm-upgrade -d
						'''
					}
				}
				stage('Prod') {
					when { branch 'master' }
					steps {
						sh '''
						'''
					}
				}
			}
		}
	}
}