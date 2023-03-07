timestamps {
  node('regular-memory-node') {
    stage('checkout') {
      scmInfo = checkout scm
      println("${scmInfo}")
      env.GIT_BRANCH = scmInfo.GIT_BRANCH
    }

    params = [
      string(name: 'test_branch', value: 'origin/pr/' + scmInfo.GIT_BRANCH.substring(3) + '/merge')
    ]
    stage('Jenkins Regression Tests') {
      build job: 'SnowparkClientRegressRunner-PC',parameters: params
    }
  }
}
