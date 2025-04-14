plugins {
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.serialization") version "2.1.0"

}

group = "pl.jacekgajek"
version = "0.0.1"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    // Provided dependencies (compileOnly for Gradle to replicate Maven's "provided" scope)
    compileOnly("org.jetbrains.kotlin:kotlin-reflect:2.1.0")
    compileOnly("org.jetbrains.kotlin:kotlin-stdlib:2.1.20")
    compileOnly("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")
    implementation("io.github.oshai:kotlin-logging-jvm:6.0.9")

    // Regular dependencies
    implementation("aws.sdk.kotlin:sqs-jvm:1.4.5")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:3.0.3")
    implementation("com.sksamuel.tabby:tabby-fp-jvm:2.1.7")

    // Test dependencies
    testImplementation("io.kotest:kotest-runner-junit5-jvm:5.9.1")
    testImplementation("io.kotest:kotest-assertions-core-jvm:5.9.1")
    testImplementation("io.kotest:kotest-property-jvm:5.9.1")
    testImplementation("org.jetbrains.kotlin:kotlin-test:2.1.0")
    testImplementation("io.mockk:mockk-jvm:1.13.13")

    // ByteBuddy (for mocking libraries, if necessary)
    implementation("net.bytebuddy:byte-buddy:1.15.1")
}

tasks {
    // Set the Kotlin source directories
    withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            freeCompilerArgs.add("-Xopt-in=kotlin.time.ExperimentalTime")
        }
    }

    // Test using JUnit 5 (Kotest compatible with JUnit 5)
    test {
        useJUnitPlatform()
    }
}
