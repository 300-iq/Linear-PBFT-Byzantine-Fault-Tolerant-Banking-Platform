import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    id("java")
    application
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

dependencies {
    implementation(project(":modules:common"))

    implementation("info.picocli:picocli:4.7.6")
    annotationProcessor("info.picocli:picocli-codegen:4.7.6")
}

application {
    mainClass.set("pbft.client.ClientMain")
}

