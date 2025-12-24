import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    id("java")
    application
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

application {
    mainClass.set("pbft.replica.ReplicaMain")
}

dependencies {
    implementation(project(":modules:common"))

    // CLI
    implementation("info.picocli:picocli:4.7.6")
    annotationProcessor("info.picocli:picocli-codegen:4.7.6")
}
