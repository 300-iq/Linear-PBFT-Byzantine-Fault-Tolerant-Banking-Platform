rootProject.name = "PBFT"
include(":modules:common", ":modules:replica", ":modules:client")
project(":modules:common").projectDir = file("Modules/common")
project(":modules:replica").projectDir = file("Modules/replica")
project(":modules:client").projectDir = file("Modules/client")