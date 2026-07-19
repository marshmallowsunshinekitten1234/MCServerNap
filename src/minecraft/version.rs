use std::fmt;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InitialProtocol {
    OptionalUuid,
    RequiredUuid,
    RequiredUuidAndTransfer,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct VersionProfile {
    name: &'static str,
    protocol: i32,
    initial_protocol: InitialProtocol,
}

impl VersionProfile {
    const fn new(name: &'static str, protocol: i32, initial_protocol: InitialProtocol) -> Self {
        Self {
            name,
            protocol,
            initial_protocol,
        }
    }
}

const OPTIONAL_UUID: InitialProtocol = InitialProtocol::OptionalUuid;
const REQUIRED_UUID: InitialProtocol = InitialProtocol::RequiredUuid;
const REQUIRED_UUID_AND_TRANSFER: InitialProtocol = InitialProtocol::RequiredUuidAndTransfer;

// Release catalogue: https://piston-meta.mojang.com/mc/game/version_manifest_v2.json
// Protocol mappings/layouts: https://github.com/PrismarineJS/minecraft-data/tree/master/data/pc
// Keep entries in release order; the final entry is the default.
const SUPPORTED_VERSIONS: &[VersionProfile] = &[
    VersionProfile::new("1.20.1", 763, OPTIONAL_UUID),
    VersionProfile::new("1.20.2", 764, REQUIRED_UUID),
    VersionProfile::new("1.20.3", 765, REQUIRED_UUID),
    VersionProfile::new("1.20.4", 765, REQUIRED_UUID),
    VersionProfile::new("1.20.5", 766, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.20.6", 766, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21", 767, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.1", 767, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.2", 768, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.3", 768, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.4", 769, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.5", 770, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.6", 771, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.7", 772, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.8", 772, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.9", 773, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.10", 773, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("1.21.11", 774, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("26.1", 775, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("26.1.1", 775, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("26.1.2", 775, REQUIRED_UUID_AND_TRANSFER),
    VersionProfile::new("26.2", 776, REQUIRED_UUID_AND_TRANSFER),
];

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct MinecraftVersion(VersionProfile);

impl MinecraftVersion {
    #[must_use]
    pub(crate) const fn name(self) -> &'static str {
        self.0.name
    }

    #[must_use]
    pub(crate) const fn protocol(self) -> i32 {
        self.0.protocol
    }

    pub(super) const fn initial_protocol(self) -> InitialProtocol {
        self.0.initial_protocol
    }

    pub(crate) fn supported() -> impl DoubleEndedIterator<Item = Self> + ExactSizeIterator {
        SUPPORTED_VERSIONS.iter().copied().map(Self)
    }

    #[cfg(test)]
    pub(crate) const fn latest() -> Self {
        Self(SUPPORTED_VERSIONS[SUPPORTED_VERSIONS.len() - 1])
    }

    fn from_name(name: &str) -> Option<Self> {
        SUPPORTED_VERSIONS
            .iter()
            .find(|profile| profile.name == name)
            .copied()
            .map(Self)
    }
}

impl fmt::Debug for MinecraftVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("MinecraftVersion")
            .field(&self.name())
            .finish()
    }
}

impl Serialize for MinecraftVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for MinecraftVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(MinecraftVersionVisitor)
    }
}

struct MinecraftVersionVisitor;

impl Visitor<'_> for MinecraftVersionVisitor {
    type Value = MinecraftVersion;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a supported Minecraft Java release")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        MinecraftVersion::from_name(value).ok_or_else(|| {
            let supported = MinecraftVersion::supported()
                .map(MinecraftVersion::name)
                .collect::<Vec<_>>()
                .join(", ");
            E::custom(format!(
                "unsupported Minecraft Java release {value:?}; expected one of {supported}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct VersionOnly {
        minecraft_version: MinecraftVersion,
    }

    #[test]
    fn catalogue_is_ordered_and_unique() {
        let versions = MinecraftVersion::supported().collect::<Vec<_>>();
        let first = versions.first().expect("first version");
        let last = versions.last().expect("last version");
        assert_eq!((first.name(), first.protocol()), ("1.20.1", 763));
        assert_eq!((last.name(), last.protocol()), ("26.2", 776));

        let names = versions
            .iter()
            .map(|version| version.name())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), versions.len());

        assert!(
            versions
                .windows(2)
                .all(|pair| pair[0].protocol() <= pair[1].protocol())
        );
    }

    #[test]
    fn catalogue_assigns_initial_protocols_by_protocol_number() {
        for version in MinecraftVersion::supported() {
            match version.initial_protocol() {
                InitialProtocol::OptionalUuid => assert_eq!(version.protocol(), 763),
                InitialProtocol::RequiredUuid => {
                    assert!((764..=765).contains(&version.protocol()));
                }
                InitialProtocol::RequiredUuidAndTransfer => {
                    assert!(version.protocol() >= 766);
                }
            }
        }
    }

    #[test]
    fn every_release_roundtrips_through_toml() {
        for minecraft_version in MinecraftVersion::supported() {
            let value = VersionOnly { minecraft_version };
            let serialized = toml::to_string(&value).expect("version should serialize");
            let deserialized: VersionOnly =
                toml::from_str(&serialized).expect("version should deserialize");
            assert_eq!(deserialized.minecraft_version, minecraft_version);
        }
    }

    #[test]
    fn latest_release_is_the_default_profile() {
        assert_eq!(MinecraftVersion::latest().name(), "26.2");
        assert!(toml::from_str::<VersionOnly>("minecraft_version = \"1.20\"").is_err());
    }
}
