import com.google.protobuf.gradle.*
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.JavaExec

plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
    application
}

application {
    mainClass.set("pbft.common.tools.KeyGenMain")
}


java {
    toolchain { languageVersion.set(org.gradle.jvm.toolchain.JavaLanguageVersion.of(21)) }
}

val grpcVersion = "1.66.0"
val protobufVersion = "3.25.3"

dependencies {
    api("io.grpc:grpc-netty:$grpcVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")

    api("org.slf4j:slf4j-api:2.0.13")
    implementation("ch.qos.logback:logback-classic:1.5.6")

    implementation("org.bouncycastle:bcprov-jdk18on:1.78.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.17.2")

    implementation("info.picocli:picocli:4.7.6")
    annotationProcessor("info.picocli:picocli-codegen:4.7.6")



    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.mockito:mockito-core:5.12.0")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

sourceSets {
    getByName("main") {
        proto {
            srcDir("${project.rootDir}/protos")
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins { id("grpc") }
        }
    }
}

tasks.withType<Jar>().configureEach {
    from(layout.buildDirectory.dir("generated/source/proto/main/java"))
    from(layout.buildDirectory.dir("generated/source/proto/main/grpc"))
}


tasks.register<JavaExec>("keygenAll") {
    group = "pbft-tools"
    description = "Generate Ed25519 keys for replicas n1..n7 into secrets/replicas/*"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("pbft.common.tools.KeyGenMain")
    val out = rootProject.layout.projectDirectory.dir("secrets").asFile.absolutePath
    args("--out", out, "--batch-replicas")
}


tasks.register<JavaExec>("keygenReplica") {
    group = "pbft-tools"
    description = "Generate Ed25519 keys for a single replica id (e.g., -Pid=n3)"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("pbft.common.tools.KeyGenMain")
    val rid = (project.findProperty("id") as String?) ?: "n1"
    val outProp = (project.findProperty("out") as String?)
    val defaultOut = rootProject.layout.projectDirectory.dir("secrets/replicas/$rid").asFile.absolutePath
    val out = outProp ?: defaultOut
    args("--out", out, "--id", rid)
}

tasks.register("keygenClientsAll") {
    group = "pbft-tools"
    description = "Generate Ed25519 keys for clients A..J into secrets/clients/*"
    doLast {
        for (c in 'A'..'J') {
            javaexec {
                classpath = sourceSets.main.get().runtimeClasspath
                mainClass.set("pbft.common.tools.KeyGenMain")
                val out = rootProject.layout.projectDirectory.dir("secrets/clients/$c").asFile.absolutePath
                args("--out", out, "--id", "$c")
            }
        }
    }
}

tasks.register<JavaExec>("keygenClient") {
    group = "pbft-tools"
    description = "Generate Ed25519 keys for a single client id (e.g., -Pid=E)"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("pbft.common.tools.KeyGenMain")
    val cid = (project.findProperty("id") as String?) ?: "A"
    val outProp = (project.findProperty("out") as String?)
    val defaultOut = rootProject.layout.projectDirectory.dir("secrets/clients/$cid").asFile.absolutePath
    val out = outProp ?: defaultOut
    args("--out", out, "--id", cid)
}
