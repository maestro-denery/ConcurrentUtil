plugins {
    java
    `maven-publish`
    id("io.github.reyerizo.gradle.jcstress") version "0.8.13"
    id("me.champeau.jmh") version "0.6.6"
}

repositories {
    mavenLocal()
    mavenCentral()
}

group = "ca.spottedleaf.concurrentutil"
version = "1.0.0-SNAPSHOT"
description = "High-Performance thread-safe utilities for Java"
java.sourceCompatibility = JavaVersion.VERSION_17

dependencies {
    jmh("org.openjdk.jmh:jmh-core:1.35")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.35")
}

java {
    withSourcesJar()
}

publishing {
    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
}

tasks {
    withType<JavaCompile>() {
        options.compilerArgs.addAll(listOf("--add-exports", "java.base/jdk.internal.vm.annotation=ALL-UNNAMED"))
        options.encoding = "UTF-8"
    }

    jar {
        from(rootProject.file("LICENSE"))
    }

    jcstress {
        jcstressDependency = "org.openjdk.jcstress:jcstress-core:0.15"
    }

    jmh {
        fork.set(2)
        //jvmArgs = ['Custom JVM args to use when forking.']
        //jvmArgsAppend = ['Custom JVM args to use when forking (append these)']
        //jvmArgsPrepend =[ 'Custom JVM args to use when forking (prepend these)']
        //humanOutputFile = project.file("${project.buildDir}/results/jmh/human.txt") // human-readable output file
        //resultsFile = project.file("${project.buildDir}/results/jmh/results.txt") // results file
        //profilers = [] // Use profilers to collect additional data. Supported profilers: [cl, comp, gc, stack, perf, perfnorm, perfasm, xperf, xperfasm, hs_cl, hs_comp, hs_gc, hs_rt, hs_thr, async]
        //timeOnIteration = '1s' // Time to spend at each measurement iteration.
        resultFormat.set("CSV") // Result format type (one of CSV, JSON, NONE, SCSV, TEXT)
        //synchronizeIterations = false // Synchronize iterations?
        threads.set(1) // Number of worker threads to run with.
        //timeout = '1s' // Timeout for benchmark iteration.
        verbosity.set("NORMAL") // Verbosity mode. Available modes are: [SILENT, NORMAL, EXTRA]

        zip64.set(true) // Use ZIP64 format for bigger archives
        duplicateClassesStrategy.set(DuplicatesStrategy.FAIL) // Strategy to apply when encountring duplicate classes during creation of the fat jar (i.e. while executing jmhJar task)
    }
}
