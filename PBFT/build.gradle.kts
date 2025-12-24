import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.JavaVersion

plugins {
}

allprojects {
    group = "pbft"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
        google()
    }
}

subprojects {

    plugins.apply("java")
    plugins.apply("idea")

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    dependencies {
        add("testImplementation", platform("org.junit:junit-bom:5.11.0"))
        add("testImplementation", "org.junit.jupiter:junit-jupiter")
    }
}

