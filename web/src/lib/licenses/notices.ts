/**
 * The licenses of the third-party code reseek ships, as `just licenses` writes them into
 * third-party.json: the crates the two programs link and the npm packages the UI bundles
 */
export type Notices = {
  /** each license text once; a package names its texts by their place here */
  texts: string[];
  /** a group per license, the one with the most packages first */
  licenses: NoticeGroup[];
};

export type NoticeGroup = {
  /** the license's SPDX identifier, or the expression an npm package's manifest gives */
  id: string;
  /** by name, then version */
  packages: NoticePackage[];
};

export type NoticePackage = {
  name: string;
  version: string;
  /** `crates.io` or `npm` */
  registry: string;
  /** the package's texts for this license, as places in `texts` */
  texts: number[];
};
