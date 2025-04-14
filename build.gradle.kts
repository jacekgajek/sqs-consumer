plugins {
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.serialization") version "2.1.0"

    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    `maven-publish`
    signing
}

val theGroup = "io.github.jacekgajek"
val theArtifact = "sqs-consumer"
val theVersion = "0.0.1"

group = theGroup
version = theVersion

repositories {
    mavenCentral()
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
        }
    }
}
publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = theGroup
            artifactId = theArtifact
            version = theVersion

            from(components["java"])

            pom {
                signing {
                    sign(publishing.publications["maven"])
                }
                name.set("SQS Consumer")
                description.set("Simple AWS SQS consumer for Kotlin which emits events as a Flow.")
                url.set("https://github.com/jacekgajek/sqs-consumer")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("jacekgajek")
                        name.set("Jacek Gajek")
                        email.set("jacek.s.gajek at gmail.com")
                        url.set("https://github.com/jacekgajek")
                        organization.set("jacekgajek")
                        organizationUrl.set("https://github.com/jacekgajek")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/jacekgajek/sqs-consumer.git")
                    developerConnection.set("scm:git:ssh://github.com/jacekgajek/sqs-consumer.git")
                    url.set("https://github.com/jacekgajek/sqs-consumer")
                }
            }
        }
    }
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
    testImplementation("io.mockk:mockk-jvm:1.14.0")

    // ByteBuddy (for mocking libraries, if necessary)
    implementation("net.bytebuddy:byte-buddy:1.15.1")
}

signing {
    sign(publishing.publications["maven"])
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
