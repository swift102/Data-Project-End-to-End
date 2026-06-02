Certainly. Here is the full governance documentation in Markdown, rewritten without emojis.

---

# Fabric Workspace Role Assignment: Governance Review

**Prepared by:** Innocent Nhamo (Data Analyst & Governance) 
**Date:** 2026-05-27
**Workspace:** Keystone Project Workspace

## 1. Objective

This document provides a governance review of the current Microsoft Fabric workspace role assignments for the KeyStone  project team. The review is conducted against each member's stated role and area of inquiry, applying the principle of least-privilege access. The goal is to validate that every team member has the permissions necessary to perform their duties, while highlighting any misalignments that pose a security risk or operational blocker.

## 2. Workspace Roles and Capabilities (Summary)

| Capability | Admin | Member | Contributor | Viewer |
|---|---|---|---|---|
| Manage workspace membership & settings | Yes | No | No | No |
| Create, edit, and delete content | Yes | Yes | Yes | No |
| Publish reports & apps | Yes | Yes | No | No |
| Share items with others | Yes | Yes | No | No |
| View and interact with content | Yes | Yes | Yes | Yes |

## 3. Current Role Assignments and Team Structure

| Name | Email | Role | Team / Department | Area of Inquiry |
|---|---|---|---|---|
| Vincent Chitsike | vincent@KeyStone .onmicrosoft.com | Data Engineer (Overall Team Lead) | Data Engineering | Project direction, priorities, approvals |
| Nomusa Lembede | nomusa@KeyStone .onmicrosoft.com | Analytics Engineer (Team Lead) | Analytics | Pipelines, ingestion, storage, transformations, dashboards |
| Innocent Nhamo | KeyStone @KeyStone .onmicrosoft.com | Data Analyst & Governance | Analytics | Reports, dashboards, KPIs, analysis, governance |
| Fulufhelo Shavhani | fulufhelo@KeyStone .onmicrosoft.com | ML Ops (Team Lead) | Data | Reports, dashboards, KPIs, analysis |
| Tebatso Mamabolo | tebatso@KeyStone .onmicrosoft.com | Business Intelligence Analyst | Data | Reports, dashboards, KPIs, analysis |
| Tebogo Lesedi | tebogo@KeyStone .onmicrosoft.com | ML Ops | Data | Reports, dashboards, KPIs, analysis |
| Philisiwe Msibi | philisiwe@KeyStone .onmicrosoft.com | Data Engineer | Data | Pipelines, ingestion, storage, transformations  |

## 4. Individual Role Justifications and Findings

### 4.1 Vincent Chitsike — Admin

**Current workspace role:** Admin
**Justification:** As the Overall Team Lead, Vincent requires full administrative control over the workspace. This includes managing membership, modifying workspace settings, and delegating permissions. The Admin role is the only role that provides these capabilities.
**Finding:** The current assignment is correct and necessary. No change required.

### 4.2 Innocent Nhamo — Member

**Current workspace role:** Member
**Justification:** My responsibilities span data analysis and governance, including auditing content, ensuring data quality, and sharing reports or semantic models with stakeholders. The Member role permits viewing, editing, deleting, publishing reports, and sharing items. I do not require the ability to manage workspace membership or settings, making Admin excessive. Member provides the appropriate balance of capability and security.
**Finding:** The current assignment is appropriate. No change required.

### 4.3 Nomusa Lembede — Contributor (Action Required)

**Current workspace role:** Contributor
**Justification:** Nomusa leads the Analytics team, which is responsible for pipelines, data ingestion, storage, and transformations. A Team Lead inevitably needs to share semantic models, dataflows, notebooks, and publish coordinated assets to other teams or stakeholders. The Contributor role explicitly does not permit sharing items or publishing reports; these are Member-level privileges. Restricting Nomusa to Contributor will block her ability to disseminate work and collaborate effectively outside her immediate team.
**Finding:** Misaligned. The Contributor role is insufficient for a Team Lead.
**Recommendation:** Elevate Nomusa to Member.

### 4.4 Fulufhelo Shavhani — Contributor (Action Required)

**Current workspace role:** Contributor
**Justification:** Fulufhelo leads the Data team, which produces reports, dashboards, KPIs, and analysis. Like Nomusa, a Team Lead must be able to share dashboards, apps, and outputs with decision-makers and other teams. The Contributor role prevents sharing, creating a bottleneck where Fulufhelo cannot directly distribute the work his team produces.
**Finding:** Misaligned. The Contributor role is insufficient for a Team Lead.
**Recommendation:** Elevate Fulufhelo to Member.

### 4.5 Tebatso Mamabolo — Viewer (Action Required)

**Current workspace role:** Viewer
**Justification:** Tebatso's core responsibility is business intelligence analysis, including building reports, dashboards, and defining KPIs. This work requires the ability to create and modify content such as reports, dataflows, and semantic models. The Viewer role is strictly read-only; it does not permit any content creation or editing. Assigning Viewer to a BI Analyst completely prevents him from performing his job.

To address the question of whether a BI/BA analyst truly needs to create content: yes, this is fundamental. A BI Analyst's workflow involves building reports, authoring DAX measures, designing dashboard layouts, and often creating dataflows or exploratory notebooks. While interaction with and research on existing data is part of the role, the output of that research is always a new or modified artifact. Without create/edit permissions, an analyst is merely a passive consumer, which does not align with the stated responsibilities.

The principle of least privilege suggests starting with Contributor rather than Member. Contributor allows full content creation and editing but reserves sharing and publishing rights for review by a Team Lead or governance. This maintains a controlled release process.
**Finding:** Critically misaligned. The Viewer role is incompatible with the BI Analyst function.
**Recommendation:** Change Tebatso's role to Contributor. If a future need to share reports independently arises, a Member upgrade can be evaluated.

### 4.6 Tebogo Lesedi — Contributor

**Current workspace role:** Contributor
**Justification:** Tebogo works in ML Ops, building and maintaining machine learning artifacts and associated reports. Contributor permits full creation, editing, and deletion of content. As a non-lead individual contributor, not having sharing or publishing rights is a reasonable boundary that follows least-privilege principles. Should regular sharing become a requirement, a role change can be considered.
**Finding:** The current assignment is appropriate. No change required.

### 4.7 Uhone Rasifudi — Contributor

**Current workspace role:** Contributor
**Justification:** Uhone focuses on data engineering tasks including ingestion, storage, and transformations. Contributor provides the necessary create, edit, and delete capabilities. As a non-lead role, restricting sharing and publishing privileges by default is sound practice.
**Finding:** The current assignment is appropriate. No change required.

## 5. Summary of Required Changes

| Person | Current Role | Required Role | Reason |
|---|---|---|---|
| Vincent Chitsike | Admin | Admin | No change |
| Innocent Nhamo | Member | Member | No change |
| Nomusa Lembede | Contributor | Member | Team Lead needs share/publish |
| Fulufhelo Shavhani | Contributor | Member | Team Lead needs share/publish |
| Tebatso Mamabolo | Viewer | Contributor | Analyst requires create/edit |
| Tebogo Lesedi | Contributor | Contributor | No change |
| Uhone Rasifudi | Contributor | Contributor | No change |

## 6. Governance Statement

This review was conducted using real-world role-based access control principles, treating our project environment as a simulation of a production data workspace. Three role misalignments were identified that would cause significant operational blockers in a live setting. The recommended changes ensure that every team member has the permissions required to fulfill their responsibilities without granting unnecessary privileges. All changes are traceable to the justifications above and align with the project's learning objective of practicing sound data governance.
