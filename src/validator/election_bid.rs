use crate::keystore::Election;
use anyhow::Result;
use ever_block::{write_boc, BuilderData, IBitstring, KeyId, UInt256};

const STAKE_REQUEST_TAG: u32 = 0x654C5074;
const NEW_STAKE_OP: u32 = 0x4E73744B;
const MAX_FACTOR_MIN: u32 = 1 << 16;
const MAX_FACTOR_MAX: u32 = 100 << 16;

#[derive(Debug, thiserror::Error)]
#[error("max factor {0} is not within 1.0..100.0 (16.16 fixed point)")]
pub struct InvalidMaxFactor(pub u32);

/// The elector's max factor, 16.16 fixed point: a stake is never signed with one out of range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxFactor(u32);

impl TryFrom<u32> for MaxFactor {
    type Error = InvalidMaxFactor;

    fn try_from(value: u32) -> Result<Self, InvalidMaxFactor> {
        if !(MAX_FACTOR_MIN..=MAX_FACTOR_MAX).contains(&value) {
            return Err(InvalidMaxFactor(value));
        }

        Ok(Self(value))
    }
}

pub fn stake_request(
    election_id: u32,
    max_factor: MaxFactor,
    address: &UInt256,
    adnl: &KeyId,
) -> Result<Vec<u8>> {
    let mut request = BuilderData::new();

    request
        .append_u32(STAKE_REQUEST_TAG)?
        .append_u32(election_id)?
        .append_u32(max_factor.0)?
        .append_raw(address.as_slice(), 256)?
        .append_raw(adnl.data(), 256)?;

    Ok(request.data().to_vec())
}

pub fn stake_message(
    election: &Election,
    max_factor: MaxFactor,
    address: &UInt256,
    query_id: u64,
) -> Result<Vec<u8>> {
    let request = stake_request(election.election_id, max_factor, address, &election.adnl)?;
    let signature = election.key.sign(&request)?;

    let mut signature_cell = BuilderData::new();
    signature_cell.append_raw(&signature, signature.len() * 8)?;

    let mut body = BuilderData::new();

    body.append_u32(NEW_STAKE_OP)?
        .append_u64(query_id)?
        .append_raw(election.key.pub_key()?, 256)?
        .append_u32(election.election_id)?
        .append_u32(max_factor.0)?
        .append_raw(election.adnl.data(), 256)?
        .checked_append_reference(signature_cell.into_cell()?)?;

    write_boc(&body.into_cell()?)
}
